## Summary

`chartevents.value` is discarded whenever `chartevents.valuenum` is set, so every
ICU observation whose text carries meaning beyond its number is published as a
bare number. On full MIMIC-IV v2.2 this affects **27,459,516 observations across
147 itemids**, and for **4,868,018** of them the result is not merely lossy but
**ambiguous** — the same published number stands for two or more different
source findings, and nothing in the resource distinguishes them.

The generation SQL writes `valueQuantity` and `valueString` from mutually
exclusive branches on the same predicate:

```sql
-- sql/fhir_observation_chartevents.sql:69-80
, 'valueQuantity',
    CASE WHEN ce_VALUENUM IS NOT NULL THEN
        jsonb_build_object(
            'value', ce_VALUENUM
            , 'unit', ce_VALUEUOM
            , 'system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-units'
            , 'code', ce_VALUEUOM
    ) ELSE NULL END
, 'valueString',
    CASE WHEN ce_VALUENUM IS NULL THEN
        ce_VALUE
    ELSE NULL END
```

`IS NOT NULL` and `IS NULL` partition the rows, so `ce.value` survives only when
there is no number — precisely when it adds nothing to the number.

I found this while porting the official `mimic-code` derived concepts onto
MIMIC-on-FHIR and hitting a concept I could not reproduce.

## The sharpest case: `itemid = 229381` (Orientation)

Orientation is recorded as one row per domain the patient is oriented to. All
seven share `valuenum = 1`; the domain lives entirely in the text:

| `valuenum` | distinct `value` | rows |
|---|---|---|
| `1` | `City`, `Month`, `Name`, `Place`, `Season`, `State`, `Year` | 1,854,119 |

Those rows are typically charted together, so the collapse happens **inside a
single charttime**. Grouping the source on the ETL's own reference key
(`stay_id`, `charttime`, `itemid`, `valuenum`, `valueuom`):

```
colliding groups : 302,981
rows involved    : 1,799,714
```

Each of those 302,981 groups publishes several Observations that agree on
`subject`, `encounter`, `effectiveDateTime`, `code`, `category`, `issued` and
`valueQuantity` — every element the resource carries. They differ only in `id`.
A client asking "what was this patient oriented to?" receives up to seven
indistinguishable copies of `value = 1`.

### Worked example

Rows from one ICU stay of the full dataset (identifiers and times replaced by
placeholders), all charted at the
same instant. This is one nurse assessing one patient's orientation across
seven domains:

| subject_id | stay_id | charttime | itemid | value | valuenum | valueuom |
|---|---|---|---|---|---|---|
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | City | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | Month | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | Name | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | Place | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | Season | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | State | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | Year | 1 | *NULL* |

Seven Observations are published. Here are two of them, in full:

```jsonc
{
  "resourceType": "Observation",
  "id": "<uuidv5(ns, '<non-demo stay_id>-<charttime>-229381-City')>",
  "meta": { "profile": ["http://mimic.mit.edu/fhir/mimic/StructureDefinition/mimic-observation-chartevents"] },
  "status": "final",
  "category": [{ "coding": [{ "system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-observation-category", "code": "Neurological" }] }],
  "code": { "coding": [{ "system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-chartevents-d-items", "code": "229381", "display": "Orientation" }] },
  "subject":   { "reference": "Patient/<uuidv5(ns, '<non-demo subject_id>')>" },
  "encounter": { "reference": "Encounter/<uuidv5(ns, '<non-demo stay_id>')>" },
  "effectiveDateTime": "<charttime>",
  "issued": "<storetime>",
  "valueQuantity": { "value": 1, "system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-units" }
}
```

```jsonc
{
  "resourceType": "Observation",
  "id": "<uuidv5(ns, '<non-demo stay_id>-<charttime>-229381-Year')>",
  // ...every other element byte-identical to the above...
  "valueQuantity": { "value": 1, "system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-units" }
}
```

`City` and `Year` differ in the id and in nothing else. The seven resources are
mutually indistinguishable, and the domain — the entire content of the
assessment — appears nowhere.

`valuenum` for this itemid is **1 in all 1,854,119 rows**. The published number
is a constant. Every bit of information these observations carry is in the field
that is dropped.

With the suggested fix, the same row becomes:

```jsonc
{
  "resourceType": "Observation",
  "id": "<uuidv5(ns, '<non-demo stay_id>-<charttime>-229381-City')>",
  // ...unchanged...
  "valueQuantity": { "value": 1, "system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-units" },
  "component": [{
    "code": { "coding": [{ "system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-observation-component", "code": "sourceValue" }] },
    "valueString": "City"
  }]
}
```

Additive, and `Observation.component.value[x]` is a separate choice element
from `Observation.value[x]`, so it is legal alongside `valueQuantity`.

## Why this is a bug and not a modelling choice

The ETL already treats `value` as identity-bearing. Twelve lines above the branch
that drops it, the resource **id** is derived from it:

```sql
-- sql/fhir_observation_chartevents.sql:20-21
-- chartevents uuid dependent on 'value' to be unique (stay_id and itemid should be enough but a couple cases break this)
, uuid_generate_v5(ns_observation_ce.uuid, ce.stay_id || '-' || ce.charttime || '-' || ce.itemid || '-' || ce.value) AS uuid_CHARTEVENTS
```

The comment's "a couple cases break this" is the 302,981 groups above. So the
field is significant enough to key the primary identifier, and is then not
served in any element of the body. The pipeline asserts the field distinguishes
rows and simultaneously declines to publish it.

A side effect worth stating plainly: because the id is a UUIDv5 over the source
text, the dropped value is *recoverable by brute-forcing candidate strings
against `Observation.id`*. We know because a port of ours did exactly that
before we removed it. A clinical value should not be reachable by inverting a
hash of the primary key, and that it is reachable is an argument for serving the
field properly.

## Scope on full MIMIC-IV v2.2

Measured over all 313,645,063 `chartevents` rows.

| fact | count |
|---|---|
| rows the ETL admits | 313,645,032 |
| …published with `valueQuantity` | 122,602,749 |
| …published with `valueString` | 191,042,283 |
| rows with a number **and** text that is not that number | **27,459,516** |
| itemids affected | 147 |
| `(itemid, valuenum)` pairs where one number has several meanings | 16 |
| …over distinct itemids | 15 |
| …covering rows | **4,868,018** |

The full ambiguous list, most affected first:

| itemid | label | `valuenum` | distinct texts | rows |
|---|---|---|---|---|
| 229381 | Orientation | 1 | City, Month, Name, Place, Season, State, Year | 1,854,119 |
| 229321 | Activity / Mobility (JH-HLM) | 2 | 2a - Passive or Active ROM, 2b - Turning in bed, 2c - Lift to chair/bed | 947,627 |
| 223900 | GCS - Verbal Response | 1 | No Response, No Response-ETT | 644,776 |
| 227343 | Ambulatory aid | 0 | Bed rest, None, Nurse assist, Wheel chair | 609,975 |
| 227345 | Gait/Transferring | 0 | Bed rest, Immobile, Normal | 522,021 |
| 228337 | CAM-ICU MS Change | 0 | No, Unable to Assess | 119,074 |
| 228395 | Orientation to Place | 1 | BIDMC, Hospital | 87,255 |
| 229325 | CAM-ICU Inattention | 0 | Language Barrier, No, Preexisting Advanced Dementia | 23,608 |
| 228394 | Orientation to Person | 1 | First name, Last name | 20,839 |
| 228336 | CAM-ICU Inattention | 0 | No, Unable to Assess | 11,911 |
| 227343 | Ambulatory aid | 15 | Cane, Crutches, Walker | 7,455 |
| 228234 | PAR-Respiration | 0 | Apneic or on mechanical ventilator, N/A - pt to remain on ventilator | 6,137 |
| 228231 | PAR-Consciousness | 0 | N/A - pt to remain sedated, Not responding | 4,538 |
| 228229 | PAR-Activity | 0 | N/A - pt to remain on neuromuscular blockade, Unable to move extremities voluntarily or on command | 4,389 |
| 229742 | Activity / Mobility (RN Daily Mobility Goal) | 2 | 2a - Passive or Active ROM, 2b - Turning in bed, 2c - Lift to chair/bed | 2,411 |
| 228335 | CAM-ICU Disorganized thinking | 0 | No, Unable to Assess | 1,883 |

The pattern is consistent: these are scored assessment instruments — Morse fall
scale, CAM-ICU delirium, JH-HLM mobility, PAR — where the number is the score
and the text is the finding that earned it. Several distinctions are clinically
load-bearing. `CAM-ICU MS Change` scoring `0` means either "no mental status
change" or "could not assess the patient"; those are different facts and the
published resource gives the same answer for both.

## Impact on an official `mimic-code` concept

`mimic-iv/concepts/measurement/gcs.sql` reads the text directly:

```sql
-- :33  verbal score
WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 0
WHEN ce.itemid = 223900 THEN ce.valuenum
-- :45  intubation flag
WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 1
```

`'No Response-ETT'` (intubated, cannot speak) and `'No Response'` (unresponsive)
both carry `valuenum = 1`:

| `itemid = 223900` | rows |
|---|---|
| total | 1,627,786 |
| with a non-NULL `valuenum` | 1,627,786 (100%) |
| `value = 'No Response-ETT'` | 573,198 |
| `value = 'No Response'` | 71,578 |
| published at `valueQuantity = 1` | 644,776 |

So 40% of GCS verbal observations land on a number that means two different
things. Three of the concept's eight outputs depend on the distinction:
`gcs_unable` *is* the flag; `gcs_verbal` scores ETT rows `0` rather than `1`;
and a verbal of `0` forces `gcs` to the literal `15`. The concept also carries
components forward over a 6-hour window, so a misread ETT row corrupts later
rows too. Against MIMIC-on-FHIR, `gcs_unable` is not derivable at all.

For completeness, I swept `mimic-iv/concepts/**/*.sql` for every comparison
against `chartevents.value` as text. It returns four concepts, and only `gcs` is
affected — the other three read itemids that carry no `valuenum`, so their text
is published as `valueString` and survives:

| concept | itemid | rows | rows with `valuenum` | text survives |
|---|---|---|---|---|
| `measurement/gcs.sql` | 223900 | 1,627,786 | 1,627,786 | **no** |
| `treatment/crrt.sql` | 224146 | 241,835 | 0 | yes |
| `treatment/code_status.sql` | 223758 | 71,141 | 0 | yes |
| `treatment/rrt.sql` | 225965 | 2,706 | 0 | yes |

The wider harm is therefore not to the concept library, which mostly escapes it,
but to the 147 itemids of assessment data that no consumer can interpret.

## This is not the house convention — `labevents` gets it right

The precedence rule itself is deliberate and documented, and I am not disputing
it. `map-observation-chartevents.md` says *"Only store valueString if
valueQuantity is not present"*, and `map-mimic-hosp-other.md` explains why:
*"Only one of valueString or valueQuantity can be present. So priority is given
to valueQuantity when it is present"*. That is correct — `value[x]` is a choice
type.

What is not the convention is discarding the text as a result. `labevents`
faces the identical problem — `value`, `valuenum`, `valueuom`, plus `comments`
— and solves it three ways:

```sql
-- fhir_observation_labevents.sql:27-37
, CASE
    WHEN lab.valuenum IS NOT NULL THEN lab.valuenum
    WHEN value ~ '[a-zA-Z]' THEN NULL          -- text-bearing values are FORCED
    WHEN value ~ '[^\x00-\x7F]' THEN NULL      -- down the valueString path
    WHEN value LIKE '%<=%' THEN CAST(split_part(lab.value,'<=',2) AS NUMERIC)
    ...
  END as lab_VALUENUM
```

```sql
-- fhir_observation_labevents.sql:156-162
-- Add clinical notes
, 'note',
    CASE WHEN lab_COMMENTS IS NOT NULL THEN
        jsonb_build_array(jsonb_build_object('text', lab_COMMENTS))
    ELSE NULL END
```

Any value containing letters is forced to a NULL `valuenum` so that it takes
the `valueString` branch; comparators are preserved as
`valueQuantity.comparator`; and `comments` is written to `note`
**unconditionally**, regardless of what `value[x]` ended up being. The IG states
that intent outright — *"Always capture the comment, but does not always match
valueString"*.

The measured outcome of that care, over all 118,171,367 `labevents` rows:

| | `labevents` | `chartevents` |
|---|---|---|
| rows published with a Quantity | 101,102,206 | 122,602,749 |
| …whose dropped text is not the number | 6,938,214 | 27,459,516 |
| …of those, MIMIC's `___` de-identification marker | 6,938,193 | — |
| …of those, text carrying real information | **21** | **27,459,516** |
| ambiguous `(itemid, value)` pairs | **0** | **16** |
| rows on an ambiguous value | **0** | **4,868,018** |

The 21 residual `labevents` rows are trailing-period typos — `value = '4.'`
with `valuenum = 4.0` — which carry nothing. **`labevents` loses no information
at all. `chartevents` loses 27.4 million rows' worth.**

The same escape hatches exist elsewhere: `micro_test` routes organisms through
`hasMember` so comments resurface downstream, `micro_susc` writes `note`
unconditionally, and `vitalsigns` pushes the second measurement of a blood
pressure into `Observation.component` — the exact mechanism proposed as the fix
here. `fhir_observation_chartevents.sql` is the only Observation generator in
the repo with a losing text column and no escape hatch: the emitted resource has
no `note`, no `component`, and no `extension`.

The profile permits the fix today. `SD_MimicObservationChartevents.fsh`
constrains `value[x] only string or Quantity` but leaves `note` and `component`
at base `0..*`, so nothing in the IG needs to change.

Finally, `git log -S"valueString" -- sql/fhir_observation_chartevents.sql`
returns exactly one commit — `403165f`, the file's first version, December 2021
— so this branch has never been revisited, while `labevents` acquired `note`,
comparator parsing and `dataAbsentReason` over subsequent commits. This reads
as an early file that the later mitigations were simply never back-applied to.

## Reproduction

Everything above comes from the PhysioNet MIMIC-IV v2.2 distribution and needs
nothing but DuckDB. The published side is derived from the generation SQL above
rather than read out of a warehouse, so the check is independent of how any
particular consumer loaded the release.

```sql
-- one number, seven meanings
SELECT valuenum, count(DISTINCT value) AS n_texts,
       list_sort(list(DISTINCT value)) AS texts, count(*) AS n_rows
FROM read_csv('mimic-iv-2.2/icu/chartevents.csv.gz', header = true)
WHERE itemid = 229381
GROUP BY valuenum;

-- rows that become Observations identical except for id
SELECT count(*) AS colliding_groups, sum(n_rows) AS rows_involved
FROM (
    SELECT count(*) AS n_rows
    FROM read_csv('mimic-iv-2.2/icu/chartevents.csv.gz', header = true)
    WHERE valuenum IS NOT NULL AND value IS NOT NULL
    GROUP BY stay_id, charttime, itemid, valuenum, valueuom
    HAVING count(DISTINCT value) > 1
);

-- the general class: every ambiguous (itemid, valuenum) pair
SELECT ce.itemid, di.label, ce.valuenum,
       count(DISTINCT ce.value) AS n_distinct_text,
       list_sort(list(DISTINCT ce.value)) AS texts,
       count(*) AS n_rows
FROM read_csv('mimic-iv-2.2/icu/chartevents.csv.gz', header = true) ce
JOIN read_csv('mimic-iv-2.2/icu/d_items.csv.gz', header = true) di
  ON di.itemid = ce.itemid
WHERE ce.valuenum IS NOT NULL AND ce.value IS NOT NULL
GROUP BY ce.itemid, di.label, ce.valuenum
HAVING count(DISTINCT ce.value) > 1
ORDER BY n_rows DESC;
```

Note when checking these figures that `chartevents.valuenum` is double
precision. Reading it single precision merges distinct values and inflates the
ambiguity counts.

## Suggested fix

**Serve the source text in `Observation.component`.** Keep `valueQuantity` as
the primary value and add a component carrying the verbatim `ce.value` — a
component code of `sourceValue` in a MIMIC-proprietary system with
`valueString`. Fully additive, invisible to existing consumers, and it restores
exactly what the derived concepts need:

```sql
, 'component',
    CASE WHEN ce_VALUENUM IS NOT NULL AND ce_VALUE IS NOT NULL THEN
        jsonb_build_array(jsonb_build_object(
            'code', jsonb_build_object(
                'coding', jsonb_build_array(jsonb_build_object(
                    'system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-observation-component'
                    , 'code', 'sourceValue'
                ))
            )
            , 'valueString', ce_VALUE
        ))
    ELSE NULL END
```

One wrinkle: there is no `mimic-observation-component` CodeSystem in the IG
today, so a new `sourceValue` code would be a new terminology artifact. If that
is unwelcome, the same component can reuse the existing
`mimic-chartevents-d-items` coding — the component then reads as "the source
representation of this item" and no new CodeSystem is needed.

### Why `component` rather than `note`

`note` is the obvious alternative — `labevents` and `micro_susc` both use it,
and it needs no new terminology. But the repo's existing convention is organised
by *what the field is*, not by whether it happens to be text.

`note` occurs exactly twice in `sql/`:

```
sql/fhir_observation_labevents.sql:157   , 'note',        -- lab_COMMENTS
sql/fhir_observation_micro_susc.sql:84   , 'note', CASE   -- mi_COMMENTS
```

Both are the source column literally named `comments`, and in both files the
*result* column goes elsewhere. `fhir_observation_micro_susc.sql` is the clearest
case, because it faces all three situations in one resource:

| source column | what it is | where it goes |
|---|---|---|
| `interpretation` | the result | `valueCodeableConcept` (:76) |
| `dilution_value`, `dilution_comparison` | a second result that will not fit `value[x]` | **extension** `dilution-details` (:89-106) |
| `comments` | commentary | `note` (:84) |

When the maintainers had a value that would not fit `value[x]`, they did not
reach for `note` — they minted a StructureDefinition for it. `vitalsigns` made
the same call with `component` for the second half of a blood pressure. `note`
was reserved for commentary in both files that use it.

`chartevents` has no `comments` column. Its `value` is the observation's result,
a coded menu selection — `'No Response-ETT'`, `'City'` — not commentary about
one. Routing it to `note` would be the first time in this repo that a result
column landed in an annotation.

Two practical consequences follow from that, rather than motivating it:

- **The consumers are programmatic.** `gcs.sql` needs
  `value = 'No Response-ETT'`. A ViewDefinition projects
  `component.where(code.coding.code = ...).valueString` cleanly; the same thing
  from `note[0].text` means string-matching free text.
- **`Annotation.text` is markdown**, so verbatim clinical strings round-trip
  through a markup layer.

This also weakens the terminology objection above: `micro_susc` shows the
project is willing to add an IG artifact to keep result data out of `note`. An
extension in that mould would be equally in-convention here if a new component
code is unwelcome.

All three options are purely additive and legal under the current profile
(`note`, `component` and `extension` are each unconstrained). If the maintainers
prefer `note` for its lower cost, that still fixes the data loss and I would
take it — but `component` is what the existing mappings actually point at.

Other alternatives considered:

- **`valueCodeableConcept` with `text`** for the affected items, carrying the
  item's value code plus the verbatim string, with the number moved to a
  component. Semantically the most correct, and the right answer for the
  assessment scales specifically, but a much larger change and it alters
  `value[x]` for resources already published.
- **Populate both `valueQuantity` and `valueString`.** Not available:
  `Observation.value[x]` is a choice type and only one variant may be set.
- **Narrow fix.** If a general change is out of scope, special-casing the 16
  ambiguous pairs listed above removes the cases where the published data is
  actively wrong rather than merely incomplete.

Happy to open a PR for whichever direction is preferred.

---

*Separately, and in the same file: `CAST(... AS TIMESTAMPTZ)` at
`fhir_observation_chartevents.sql:9` shifts source timestamps that land in the
US daylight-saving spring-forward gap forward by one hour, unrecoverably. Filed
as #124; both are cases of the published resource being unable to express what
the source row said.*

*Also spotted while checking whether this pattern recurs, and probably worth its
own issue: `fhir_medication_administration_icu.sql:61-69` writes
`effectivePeriod` only when `inputevents.rate IS NOT NULL`, and
`effectiveDateTime = endtime` otherwise, so `starttime` is dropped for every
non-rate administration. That is 4,158,841 rows, of which 399,946 have a start
and end more than five minutes apart — the 99th percentile of the discarded
interval is 5.8 hours and the longest is 4.5 days. `ie_STARTTIME` is referenced
exactly once in the file, inside the branch that is skipped.*
