# Upstream bug — chartevents `value` text is discarded whenever `valuenum` is set

**Target:** `mimic-fhir` (MIMIC-on-FHIR ETL)
**File:** `sql/fhir_observation_chartevents.sql`
**Verified against:** local checkout at commit `4327224` (`main`)
**Status:** **CONFIRMED** on full MIMIC-IV v2.2. HPC job `29804685`
(authoritative); results in `hpc/results/job-29804685/`.

This is the working copy. The original draft lives at
`mimic-code/mimic-iv/concepts_fhir/BUG_chartevents_value_text_dropped.md`; this
one carries the measurement method and the full-data results, and is the source
for `ISSUE_DRAFT.md`.

---

## Summary

`Observation.valueQuantity` and `Observation.valueString` are written from
mutually exclusive branches keyed on the same predicate, so a `chartevents` row
that has **both** a numeric `valuenum` and a semantically distinct `value` text
loses the text entirely. The resulting Observation is not merely lossy — it is
**ambiguous**: two source rows meaning different things collapse onto the same
served resource content.

The clearest instance is `itemid = 223900` (GCS - Verbal Response), where
`'No Response-ETT'` (patient is intubated, cannot speak) and `'No Response'`
(patient is unresponsive) both carry `valuenum = 1` and therefore both serialise
as `valueQuantity = 1`. The distinction is clinically significant and is used by
the official derived concept, and it cannot be recovered from the served
resource.

---

## The code

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

`ce_VALUENUM IS NOT NULL` and `ce_VALUENUM IS NULL` partition the rows, so
exactly one of the two elements is ever populated. `ce.value` survives only when
there is no number — i.e. precisely when it carries no additional information
over the number.

Quotation checked against the working tree at `4327224`: the two `CASE`
expressions are at lines 69-80 and the UUID expression below is at lines 20-21,
as cited.

## Why this is a bug rather than a modelling choice

The ETL **already treats `ce.value` as identity-bearing**. Twelve lines above the
branch that discards it:

```sql
-- sql/fhir_observation_chartevents.sql:20-21
-- chartevents uuid dependent on 'value' to be unique (stay_id and itemid should be enough but a couple cases break this)
, uuid_generate_v5(ns_observation_ce.uuid, ce.stay_id || '-' || ce.charttime || '-' || ce.itemid || '-' || ce.value) AS uuid_CHARTEVENTS
```

So `value` is significant enough that the resource **id** depends on it, and yet
it is not preserved anywhere in the resource body. Two rows that differ only in
`value` get two distinct Observations with byte-identical content. That is
self-inconsistent: the ETL asserts the field distinguishes rows and then declines
to serve it.

The comment's "a couple cases break this" is itself measurable, and the
verification job counts it directly — see *Collisions* below. Where a collision
exists, the defect is not merely "the text is unavailable" but "the API serves
two resources that a client cannot tell apart except by id".

It also has an unpleasant second-order effect. Because the id is a UUIDv5 over
the source text, the discarded value is *technically* recoverable by
brute-forcing candidate strings against `Observation.id`. We know this because a
port in this repo did exactly that before we removed it. Nobody should be able to
recover a clinical value by inverting a hash of the primary key, and the fact
that it is possible is a good argument for serving the field properly.

---

## Concrete impact: the official `gcs` derived concept

`mimic-code`'s `mimic-iv/concepts/measurement/gcs.sql` depends on the exact text
in two places:

```sql
-- :33  verbal score
WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 0
WHEN ce.itemid = 223900 THEN ce.valuenum
-- :45  intubation flag
WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 1
```

Three of the concept's eight output columns depend on that sentinel:

| output | dependency |
|---|---|
| `gcs_unable` | entirely — it *is* the flag |
| `gcs_verbal` | ETT rows are scored `0` rather than `valuenum` (= 1) |
| `gcs` | a verbal of `0` forces the total score to the literal `15` |

and the effect propagates: the concept carries components forward across a
6-hour window (`COALESCE(gcsverbal, gcsverbalprev)`), so a misread ETT row also
corrupts subsequent rows in the same window.

Against MIMIC-on-FHIR, `gcs_unable` is therefore not derivable at all, and
`gcs_verbal` / `gcs` are wrong on every intubated observation.

---

## Provenance of the numbers

Two sets of numbers appear in this document and they must not be confused.

**Demo probe (provisional, superseded).** From a probe over the demo Delta
warehouse plus the demo DuckDB source, recorded in
`mimic-iv/concepts_fhir/carryover/gcs/fhir-prober.md:99-113`. These established
the mechanism, not the scope, and they are *our* numbers over *our* warehouse
build. They are retained only for comparison.

| fact | count |
|---|---|
| `itemid=223900` rows | 3,266 |
| …with non-NULL `valuenum` | 3,266 / 3,266 |
| …with `value = 'No Response-ETT'`, all `valuenum = 1` | 1,348 |
| …with `value = 'No Response'`, also `valuenum = 1` | 78 |
| served Observations for `223900` with `valueQuantity` | 3,266 / 3,266 |
| served Observations for `223900` with `valueString` | **0 / 3,266** |
| served Observations carrying the ambiguous `valueQuantity = 1` | 1,426 |

**Full cohort (authoritative).** Measured against MIMIC-IV 2.2 as distributed by
PhysioNet. See *Method* below and `hpc/results/job-29804685/`.

The demo probe held up. It understated the scope badly — it found the GCS case
and missed fourteen other itemids, one of them larger — but nothing it claimed
was wrong, and the full-data GCS proportions are close to the demo split
(demo 1,348 ETT vs 78 No-Response; full 573,198 vs 71,578).

---

## Method

Deliberately constrained so that anyone holding the PhysioNet files can re-run
it and check the arithmetic, without our warehouse, without Spark, and without a
Postgres MIMIC build. The only dependency is DuckDB.

**Source side** is read straight from `mimic-iv-2.2/icu/chartevents.csv.gz`
(313,645,063 rows). It is read a second time from a prebuilt DuckDB load of the
same CSVs, and the two readings are asserted to agree cell-for-cell on
`(itemid, value, valuenum)` before anything downstream is believed. The
agreement check exists so that the reported numbers cannot be attributed to the
convenience database.

**Served side** is *not* read out of a warehouse. It is reproduced by
re-implementing the ETL's own two `CASE` expressions, and its `WHERE` clause,
as SQL over the source rows — i.e. the report projects what
`fhir_observation_chartevents.sql` would emit, using that file as its own
specification. This is stated as a projection rather than as observed output.
It is the stronger form of evidence for an upstream report: it depends on
upstream's code and PhysioNet's data, and on nothing of ours.

Runner: `hpc/run_chartevents_value_drop.slurm` → `hpc/chartevents_value_drop.py`.

### What is measured

| # | test | output |
|---|---|---|
| 1 | ambiguous `(itemid, valuenum)` pairs — one number, several meanings | `ambiguous_pairs.csv` |
| 2 | numeric rows whose `value` is not merely the number written out | `nonnumeric_text_items.csv` |
| 3 | the `223900` value/valuenum breakdown | `gcs_verbal_223900.csv` |
| 4 | what the ETL would serve: `valueQuantity` vs `valueString` counts | `report.txt` |
| 5 | every itemid an official concept reads as text | `concept_itemid_audit.csv` |
| — | collisions: resources identical except for id | `collisions_by_item.csv` |

Test 4 replaces the original draft's "confirm against the served warehouse".
The draft proposed a ViewDefinition over our Delta build; that was dropped
because it makes the central claim depend on an artefact upstream cannot
inspect.

The collision pass is the strict form of the ambiguity claim: same `stay_id`,
same `charttime`, same `itemid`, same `valuenum`, different `value`. Those rows
share every served element and differ only in the UUIDv5-derived id.

### Scope of test 5

The original draft listed four concepts and marked two "unverified". Sweeping
`mimic-iv/concepts/**/*.sql` for every comparison against `chartevents.value`
as text returns exactly those four and no others, so the list is complete:

| concept | line | itemid | draft status |
|---|---|---|---|
| `measurement/gcs.sql` | 33, 45 | `223900` | confirmed affected |
| `treatment/rrt.sql` | 100 | `225965` | unverified — most likely candidate |
| `treatment/crrt.sql` | 65-88 | `224146` | believed unaffected |
| `treatment/code_status.sql` | 21-31 | `223758` | believed unaffected |

A concept is affected iff its itemid carries a non-NULL `valuenum`, since that
is exactly the condition under which the ETL drops the text. The job decides
this per itemid rather than by inspection.

---

## Full-cohort results

Job `29804685`. Full output in `hpc/results/job-29804685/report.txt`.

### Source agreement

The two independent readings agree exactly — 0 groups present in only one, 0
groups with differing counts, 313,645,063 rows each.

One wrinkle, resolved: the prebuilt DuckDB stores `valuenum` **single
precision** while the CSV text reads as **double**, so widening it back
reproduces the binary32 rounding and `0.9` returns as `0.8999999761581421`.
The first run (`29804197`) reported a 49,071-group mismatch for this reason
alone. The comparison now runs at the coarser precision, and the FLOAT cast
merges **0** groups, so nothing is conceded by doing so. MIMIC-IV declares the
column double precision, so the double reading is the authoritative one and is
what every number below is computed from. Anyone re-checking these figures
against a single-precision copy will get inflated ambiguity counts.

### Headline

| fact | count |
|---|---|
| `chartevents` rows | 313,645,063 |
| rows the ETL admits | 313,645,032 |
| …published with `valueQuantity` | 122,602,749 |
| …published with `valueString` | 191,042,283 |
| rows with a number **and** text that is not that number | **27,459,516** |
| itemids so affected | 147 |
| ambiguous `(itemid, valuenum)` pairs | 16 |
| …over distinct itemids | 15 |
| …covering rows | **4,868,018** |

22% of all numeric chartevents observations carry text the resource cannot
express. The full pair list is in `ambiguous_pairs.csv` and is reproduced in
`ISSUE_DRAFT.md`.

### The strongest case is not GCS

`itemid = 229381` (Orientation) is larger and worse. Seven texts — `City`,
`Month`, `Name`, `Place`, `Season`, `State`, `Year` — all share `valuenum = 1`,
over 1,854,119 rows. Because the domains are charted together, the collapse
happens *within a single charttime*: grouping on the ETL's own reference key
finds **302,981 groups covering 1,799,714 rows** where several source rows
agree on every published element and differ only in the UUIDv5-derived id.

That is the concrete form of the "a couple cases break this" comment at line 20,
and it is a stronger claim than the original draft made: not merely that the
text is unavailable, but that the API serves duplicate resources a client
cannot tell apart. `229381` is the only itemid where this happens.

### GCS

| `itemid = 223900` | rows |
|---|---|
| total | 1,627,786 |
| with a non-NULL `valuenum` | 1,627,786 (100%) |
| `value = 'No Response-ETT'` | 573,198 |
| `value = 'No Response'` | 71,578 |
| published at `valueQuantity = 1` | 644,776 |

Confirmed as drafted. 40% of verbal-response observations are ambiguous.

### Test 5 — one draft prediction was wrong

The draft called `rrt` "the one to check — most likely candidate". It is not
affected: `225965` carries **no** `valuenum` on any of its 2,706 rows, so
`'In use'` is published as `valueString` and survives. Same for `crrt` (224146,
0 of 241,835) and `code_status` (223758, 0 of 71,141), as the draft guessed.

| concept | itemid | rows | rows with `valuenum` | text survives |
|---|---|---|---|---|
| `measurement/gcs.sql` | 223900 | 1,627,786 | 1,627,786 | **no** |
| `treatment/crrt.sql` | 224146 | 241,835 | 0 | yes |
| `treatment/code_status.sql` | 223758 | 71,141 | 0 | yes |
| `treatment/rrt.sql` | 225965 | 2,706 | 0 | yes |

So `gcs` is the only official concept affected, and the report should be framed
accordingly: the damage is not to the concept library, which largely escapes,
but to the 147 itemids of scored assessment data — Morse fall scale, CAM-ICU,
JH-HLM mobility, PAR — where the number is a score and the text is the finding
that earned it. `CAM-ICU MS Change = 0` meaning either "no mental status change"
or "unable to assess" is the kind of distinction being lost.

---

## Is it systemic or intentional?

Swept every SQL file in `sql/` for the same shape, checked the profiles in the
`kind-lab/mimic-profiles` submodule, and read the git history of the branch.
Answer: **the precedence rule is intentional and documented; the data loss is
not, and chartevents is the only file that suffers it.**

Separate the two claims:

1. *"Only one of `valueQuantity`/`valueString`, and Quantity wins."* Deliberate,
   written into the IG (`map-observation-chartevents.md`: "Only store
   valueString if valueQuantity is not present"), and unavoidable anyway —
   `value[x]` is a choice type. Not the bug.
2. *"Therefore the text is discarded."* Not the convention. Every other
   Observation generator with a competing text column gives it a lossless second
   home.

| file | competing text | where it goes |
|---|---|---|
| `fhir_observation_labevents.sql` | `value`, `comments` | text-bearing values forced to NULL `valuenum` so they take `valueString`; comparators to `valueQuantity.comparator`; `comments` to `note` unconditionally |
| `fhir_observation_micro_test.sql` | `comments` | organisms via `hasMember`, comments resurface downstream |
| `fhir_observation_micro_susc.sql` | `comments` | `note`, unconditionally |
| `fhir_observation_vitalsigns.sql` | second BP measurement | `Observation.component` — the mechanism proposed as the fix here |
| **`fhir_observation_chartevents.sql`** | **`value`** | **nowhere. No `note`, no `component`, no `extension`.** |

The measured contrast with labevents, which faces the identical problem:

| | `labevents` | `chartevents` |
|---|---|---|
| rows published as a Quantity | 101,102,206 | 122,602,749 |
| …dropped text that is not the number | 6,938,214 | 27,459,516 |
| …of those, the `___` de-identification marker | 6,938,193 | — |
| …of those, real information | **21** | **27,459,516** |
| ambiguous pairs | **0** | **16** |

The 21 residual labevents rows are trailing-period typos (`'4.'` with
`valuenum = 4.0`). labevents loses nothing.

Supporting evidence:

- **The profile permits the fix now.** `SD_MimicObservationChartevents.fsh` has
  `value[x] only string or Quantity`, and leaves `note`/`component` at base
  `0..*`. No IG change needed. (It also has `value[x] 1..1`, which explains the
  `AND value IS NOT NULL` filter at line 37 — chartevents cannot fall back to
  `dataAbsentReason` the way its siblings can.)
- **The branch has never been revisited.** `git log -S"valueString" --
  sql/fhir_observation_chartevents.sql` returns exactly one commit: `403165f`,
  the file's first version, 2021-12-15. labevents acquired `note`, comparator
  parsing and `dataAbsentReason` in later commits; chartevents received none of
  them. This looks like an early file the mitigations were never back-applied to.
- **The authors knew `value` disambiguates rows.** Commit `01a0839` (2022-01-07,
  "fixed primary keys") changed the UUID seed from
  `stay_id-charttime-itemid` to `stay_id-charttime-itemid-value` *to resolve
  primary-key collisions* — i.e. they hit the itemid 229381 case directly, and
  used the text for identity while withholding it from content.
- **No upstream issue covers this.** Searched `kind-lab/mimic-fhir`; #41 ("How
  to handle lab results in comments") is adjacent and is where the labevents
  `note` mitigation was decided — *"storing comments in notes makes sense"* —
  which is exactly the reasoning chartevents never received. #42 asks about the
  unmapped `warning` column on the same table. Our DST finding is #124.

### Secondary defect found by the same sweep

`sql/fhir_medication_administration_icu.sql:61-69` writes `effectivePeriod` only
when `inputevents.rate IS NOT NULL`, and `effectiveDateTime = endtime`
otherwise, so `starttime` is dropped for every non-rate administration.
`ie_STARTTIME` is referenced exactly once in the file, inside the skipped
branch. Measured: **4,158,841 rows**, of which **399,946** have start and end
more than five minutes apart; p99 of the discarded interval is 347 minutes and
the maximum is 6,531 minutes (4.5 days). Same shape as the chartevents defect —
complementary CASE branches where one branch silently drops a source column.
Worth its own issue.

---

## Suggested fixes

In rough order of preference.

**1. Write both.** Drop the `ELSE NULL` exclusivity and populate `valueString`
whenever `ce.value` is present, alongside `valueQuantity`. Simplest change, but
`Observation.value[x]` is a choice type — two variants cannot both be set on the
same element, so this is not actually legal FHIR. Noted only to be dismissed.

**2. `valueCodeableConcept` with `text`, for the affected items.** Where the
source text is a coded menu selection rather than free text, serve
`valueCodeableConcept` carrying `coding` (the item's value code, if one exists)
and `text` (the verbatim source string), and put the number in
`Observation.component` or drop it where it is derivative. Semantically the most
correct, most invasive.

**3. `Observation.component` for the source text.** Keep `valueQuantity` as the
primary value and add a component carrying the verbatim `ce.value` — e.g. a
component code of `sourceValue` in a MIMIC-proprietary system with
`valueString`. Least disruptive to existing consumers, fully additive, and it
directly restores what the derived concepts need. **This is the one I would
propose.**

**4. Minimum viable.** If a general fix is out of scope, at least special-case
the items where `(itemid, valuenum)` is provably ambiguous — the output of test
1 above is exactly that list. Narrower, but it fixes `gcs` and whatever else
test 1 turns up.

---

## What we are doing meanwhile

Not waiting on this. In our port, `gcs_unable` is declared unrepresentable and
emitted as a typed NULL, and `gcs_verbal` / `gcs` are allowed to diverge on
intubated rows with this ETL behaviour cited as the cause. See
`mimic-iv/concepts_fhir/TODO_reopen_uuid_inversion.md`.

Worth bundling into the same issue as the separate `TIMESTAMPTZ` DST-gap
finding (`fhir_observation_chartevents.sql:9`), since both are in this file and
both are "the served resource cannot express what the source row said". The
DST-gap investigation is at `debug-timezone-bug/`, and its issue draft at
`debug-timezone-bug/hpc/ISSUE_DRAFT.md`.
