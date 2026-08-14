## Summary

`chartevents.value` is discarded whenever `chartevents.valuenum` is set, so every
ICU observation whose text carries meaning beyond its number is published as a
bare number. On full MIMIC-IV v2.2 this affects **27,459,516 observations across
147 itemids**. For **4,868,018** of them the result is not merely lossy but
**ambiguous**: the same published number stands for two or more different source
findings, and nothing in the resource distinguishes them.

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

## How I ran into it

I am porting the official `mimic-code` derived concepts, which are written
against MIMIC-IV, so that they run against the mapped MIMIC-on-FHIR release
instead. `measurement/gcs.sql` does not map cleanly, and the reason is
this branch.

The concept reads the source text directly:

```sql
-- mimic-iv/concepts/measurement/gcs.sql:33   verbal score
WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 0
WHEN ce.itemid = 223900 THEN ce.valuenum
-- mimic-iv/concepts/measurement/gcs.sql:45   intubation flag
WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 1
```

`'No Response-ETT'` (intubated, cannot speak) and `'No Response'` (unresponsive)
are different clinical states that share `valuenum = 1`:

| `itemid = 223900` (GCS - Verbal Response) | rows |
|---|---|
| total | 1,627,786 |
| with a non-NULL `valuenum` | 1,627,786 (100%) |
| `value = 'No Response-ETT'` | 573,198 |
| `value = 'No Response'` | 71,578 |
| published at `valueQuantity = 1` | 644,776 |

Because every one of those rows has a `valuenum`, `valueString` is never
written, and the sentinel the concept keys on is absent from the published data.
40% of GCS verbal observations land on a number meaning two different things.

Three of the concept's eight outputs depend on the distinction: `gcs_unable`
*is* the flag; `gcs_verbal` scores ETT rows `0` rather than `1`; and a verbal of
`0` forces `gcs` to the literal `15`. The concept also carries components
forward over a 6-hour window, so a misread ETT row corrupts subsequent rows in
that window. Against MIMIC-on-FHIR, `gcs_unable` cannot be derived at all, and
in my port it is emitted as a typed NULL with this behaviour cited as the cause.

Checking whether this was specific to GCS is what turned up the rest.

## The clearest case: `itemid = 229381` (Orientation)

Orientation is charted as one row per domain the patient is oriented to. All
seven domains share `valuenum = 1` — the entire content is in the text.

Rows from one ICU stay of the full dataset (identifiers and times replaced by
placeholders), one assessment:

| subject_id | stay_id | charttime | itemid | value | valuenum | valueuom |
|---|---|---|---|---|---|---|
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | City | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | Month | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | Name | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | Place | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | Season | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | State | 1 | *NULL* |
| <non-demo subject_id> | <non-demo stay_id> | <charttime> | 229381 | Year | 1 | *NULL* |

Seven Observations are published. The first:

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

The seventh:

```jsonc
{
  "resourceType": "Observation",
  "id": "<uuidv5(ns, '<non-demo stay_id>-<charttime>-229381-Year')>",
  // every other element byte-identical to the above
  "valueQuantity": { "value": 1, "system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-units" }
}
```

`City` and `Year` differ in the `id` and in nothing else. A client asking what
the patient was oriented to receives seven indistinguishable copies of
`value = 1`.

Because these rows are charted together, the collapse happens inside a single
charttime. Grouping the source on the ETL's own reference key (`stay_id`,
`charttime`, `itemid`, `valuenum`, `valueuom`) gives **302,981 groups covering
1,799,714 rows** where the published resources agree on every element and differ
only in `id`.

`valuenum` for this itemid is **1 in all 1,854,119 rows**. The published number
is a constant, so every bit of information these observations carry is in the
dropped field.

## Why this is a bug rather than a modelling choice

The ETL already treats `value` as identity-bearing. Twelve lines above the branch
that drops it, the resource **id** is derived from it:

```sql
-- sql/fhir_observation_chartevents.sql:20-21
-- chartevents uuid dependent on 'value' to be unique (stay_id and itemid should be enough but a couple cases break this)
, uuid_generate_v5(ns_observation_ce.uuid, ce.stay_id || '-' || ce.charttime || '-' || ce.itemid || '-' || ce.value) AS uuid_CHARTEVENTS
```

That line arrived in commit `01a0839` ("fixed primary keys"), which changed the
seed from `stay_id-charttime-itemid` to include `value` *because rows were
colliding* — the 302,981 groups above are the collisions. So the field was
known to distinguish rows, was used to key the primary identifier, and is then
not served in any element of the body.

One consequence worth stating: because the id is a UUIDv5 over the source text,
the dropped value is recoverable by brute-forcing candidate strings against
`Observation.id`. A port of mine did exactly that before I removed it. A clinical
value should not be reachable by inverting a hash of the primary key.

## Scope on full MIMIC-IV v2.2

Measured over all 313,645,063 `chartevents` rows:

| fact | count |
|---|---|
| rows published with `valueQuantity` | 122,602,749 |
| …carrying text that is not the number | **27,459,516** (147 itemids) |
| `(itemid, valuenum)` pairs where one number has several meanings | **16** (15 itemids) |
| rows on such a pair | **4,868,018** |

The largest ambiguous pairs:

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

The remaining eight are smaller and of the same kind; the query below returns
all sixteen.

These are scored assessment instruments — Morse fall scale, CAM-ICU delirium,
JH-HLM mobility, PAR — where the number is the score and the text is the finding
that earned it. Several of the distinctions are clinically load-bearing:
`CAM-ICU MS Change` scoring `0` means either "no mental status change" or
"unable to assess the patient", and the published resource gives the same answer
for both.

## Other tables in the ETL already handle this correctly

The precedence rule itself is deliberate and documented, and I am not disputing
it. `map-observation-chartevents.md` says *"Only store valueString if
valueQuantity is not present"*, and `map-mimic-hosp-other.md` explains why:
*"Only one of valueString or valueQuantity can be present. So priority is given
to valueQuantity when it is present"*. That is correct — `value[x]` is a choice
type, and the exclusivity is unavoidable.

What is not the convention is losing the text as a result. `labevents` faces the
identical problem — `value`, `valuenum`, `valueuom` plus `comments` — and
protects against it:

```sql
-- sql/fhir_observation_labevents.sql:27-37
, CASE
    WHEN lab.valuenum IS NOT NULL THEN lab.valuenum
    WHEN value ~ '[a-zA-Z]' THEN NULL          -- text-bearing values are FORCED
    WHEN value ~ '[^\x00-\x7F]' THEN NULL      -- down the valueString path
    WHEN value LIKE '%<=%' THEN CAST(split_part(lab.value,'<=',2) AS NUMERIC)
    ...
  END as lab_VALUENUM
```

Any value containing letters is forced to a NULL `valuenum` so that it takes the
`valueString` branch, and comparators survive as `valueQuantity.comparator`.
Measured over all 118,171,367 `labevents` rows:

| | `labevents` | `chartevents` |
|---|---|---|
| rows published with a Quantity | 101,102,206 | 122,602,749 |
| …whose dropped text is not the number | 6,938,214 | 27,459,516 |
| …of those, MIMIC's `___` de-identification marker | 6,938,193 | — |
| …of those, text carrying real information | **21** | **27,459,516** |
| ambiguous pairs | **0** | **16** |

The 21 residual `labevents` rows are trailing-period typos (`value = '4.'` with
`valuenum = 4.0`) and carry nothing. **`labevents` loses no information at all.**

The same care appears elsewhere: `micro_test` routes organisms through
`hasMember` so their detail resurfaces downstream, `micro_susc` puts dilution
values into an extension, and `vitalsigns` puts the second half of a blood
pressure into `Observation.component`. `fhir_observation_chartevents.sql` is the
only Observation generator in the repo with a losing text column and no
alternative home for it — the emitted resource has no `component`, no
`extension`, and no `note`.

The history suggests why. `git log -S"valueString" --
sql/fhir_observation_chartevents.sql` returns exactly one commit: `403165f`, the
file's first version, December 2021. The branch has never been revisited, while
`labevents` acquired comparator parsing, `note` and `dataAbsentReason` in later
commits. This reads as an early file that the later mitigations were never
back-applied to, rather than a considered decision.

## Reproduction

Requirements:

- MIMIC-IV v2.2 from PhysioNet — `icu/chartevents.csv.gz` and `icu/d_items.csv.gz`
- DuckDB 1.5.5 for the queries as written
- `valuenum` read at double precision, as MIMIC-IV declares it; reading it
  single precision merges distinct values and inflates the ambiguity counts

The published-side figures are obtained by applying the two `CASE` expressions
quoted at the top to those rows, so the check depends only on the generation SQL
and the distributed files.

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

## Suggested fix

Keep `valueQuantity` as the primary value and give the verbatim `ce.value` a
second home. Two options, both already used in this repo, both purely additive,
and both legal under the current profile — `SD_MimicObservationChartevents.fsh`
constrains `value[x] only string or Quantity` but leaves `component` and
`extension` unconstrained, so neither needs an IG change to the profile itself.

**Option A — `Observation.component`.** The mechanism
`fhir_observation_vitalsigns.sql:146-206` already uses to carry the second half
of a blood pressure, a measurement that will not fit a single `value[x]`:

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

This needs a code for the component. There is no `mimic-observation-component`
CodeSystem today, so that would be a new terminology artifact; alternatively the
component can reuse the existing `mimic-chartevents-d-items` coding, reading as
"the source representation of this item", and nothing new is required.

**Option B — an extension.** The mechanism
`fhir_observation_micro_susc.sql:89-106` already uses for `dilution_value` and
`dilution_comparison`, via the `dilution-details` StructureDefinition:

```sql
, 'extension',
    CASE WHEN ce_VALUENUM IS NOT NULL AND ce_VALUE IS NOT NULL THEN
        jsonb_build_array(jsonb_build_object(
            'url', 'http://mimic.mit.edu/fhir/mimic/StructureDefinition/chartevents-source-value'
            , 'valueString', ce_VALUE
        ))
    ELSE NULL END
```

This needs a StructureDefinition rather than a code, which is the same size of
IG addition as `dilution-details` was.

I have no strong preference between the two; `component` is marginally easier
for consumers to project in SQL-on-FHIR, and `extension` has the closer
precedent for "a value that will not fit `value[x]`".

**`note` is not recommended.** It is the cheapest option and needs no new IG
artifact, but it does not fit the convention this repo has established. `note`
occurs exactly twice in `sql/`:

```
sql/fhir_observation_labevents.sql:157   , 'note',        -- lab_COMMENTS
sql/fhir_observation_micro_susc.sql:84   , 'note', CASE   -- mi_COMMENTS
```

Both are the source column named `comments`, and in both files the result column
goes elsewhere. `micro_susc` shows all three choices side by side —
`interpretation` to `valueCodeableConcept`, dilution to an extension, `comments`
to `note` — so when the maintainers had a value that would not fit `value[x]`,
they added an IG artifact rather than reach for an annotation. `chartevents` has
no `comments` column; its `value` is the observation's result, a coded menu
selection, not commentary about one. Putting it in `note` would also leave
consumers string-matching free text where a component or extension can be
projected directly.

### Other alternatives considered

- **`valueCodeableConcept` with `text`**, carrying the item's value code plus the
  verbatim string, with the number moved to a component. Semantically the most
  correct answer for the assessment scales, but a much larger change, and it
  alters `value[x]` for resources already published.
