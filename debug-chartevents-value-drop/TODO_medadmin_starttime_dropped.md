# TODO — `inputevents.starttime` dropped for every non-rate administration

**Status:** unfiled. Found incidentally while checking whether the chartevents
`value` defect was systemic; measured, not yet written up as an issue.
**Target:** `mimic-fhir`
**File:** `sql/fhir_medication_administration_icu.sql:61-69`
**Related:** same shape as the chartevents defect (`BUG_chartevents_value_text_dropped.md`)
and the DST-gap finding (upstream #124). All three are in the "the served
resource cannot express what the source row said" family.

---

## The code

```sql
-- sql/fhir_medication_administration_icu.sql:61-69
, 'effectivePeriod',
    CASE WHEN ie_RATE IS NOT NULL THEN
        jsonb_build_object(
            'start', ie_STARTTIME
            , 'end', ie_ENDTIME
        )
    ELSE NULL END
, 'effectiveDateTime',
    CASE WHEN ie_RATE IS NULL THEN ie_ENDTIME ELSE NULL END
```

`ie_RATE IS NOT NULL` and `ie_RATE IS NULL` partition the rows. When there is no
rate — a bolus or other non-infusion administration — the resource gets
`effectiveDateTime = endtime` and **`starttime` is written nowhere**.

Verified: `ie_STARTTIME` occurs exactly twice in the file — the declaration at
line 8, and line 64 inside the branch that is skipped.

```
$ grep -n "ie_STARTTIME" sql/fhir_medication_administration_icu.sql
8:        CAST(ie.starttime AS TIMESTAMPTZ) AS ie_STARTTIME
64:                    'start', ie_STARTTIME
```

Identical shape to the chartevents bug: two complementary `CASE` branches on one
predicate, where one branch silently drops a source column.

## Scale

Full MIMIC-IV v2.2, measured from `mimiciv_icu.inputevents`
(`hpc/results/siblings/siblings.json`, job `29805293`):

| fact | count |
|---|---|
| `inputevents` rows | 8,978,893 |
| rows with `rate IS NULL` (→ `effectiveDateTime`) | 4,158,842 |
| …where `starttime <> endtime`, i.e. a real interval is lost | **4,158,841** |
| …where the lost interval exceeds 5 minutes | 399,946 |

Only **one** of the 4.16M affected rows has `starttime = endtime`. Every other
one loses information.

Distribution of the discarded interval, in minutes:

| percentile | minutes |
|---|---|
| p50 | 1 |
| p90 | 1 |
| p99 | 347 |
| max | 6,531 |

So the median case is trivial (a one-minute bolus) but the tail is not: 399,946
administrations lose an interval longer than five minutes, the 99th percentile
discards 5.8 hours, and the longest discards 4.5 days.

## Why this is a bug and not a modelling choice

The IG's own mapping table documents `starttime` as mapped **unconditionally**
(`kind-lab/mimic-profiles`, `input/includes/map-medication-administration-icu.md`
at `3c519f3`):

```
|inputevents|starttime|MedicationAdministration.effectivePeriod.start|[MimicMedicationAdministrationICU]||
|inputevents|rate    |MedicationAdministration.effectiveDateTime     |[MimicMedicationAdministrationICU]||
|inputevents|endtime |MedicationAdministration.effectivePeriod.end   |[MimicMedicationAdministrationICU]|only set period if inputevents.rate is null|
```

Two problems:

1. The `starttime` row carries **no caveat at all** — the documented intent is
   that it is always mapped. The SQL does not do that.
2. The caveat on the `endtime` row — *"only set period if inputevents.rate is
   null"* — is **inverted** relative to the code, which sets the period when
   `rate IS NOT NULL`. The older `map-mimic-icu.md` agrees with the code
   ("Use effectivePeriod for infusions with rates"), so the newer doc is simply
   wrong.

Either way the implementation and the specification disagree, which makes this
easier to argue than the chartevents case: no one has to be persuaded that the
data matters, only that the code should match the documented mapping.

## Suggested fix

`MedicationAdministration.effective[x]` is a choice type, so the two variants
genuinely cannot coexist — but nothing forces the non-rate branch to collapse to
an instant. Write `effectivePeriod` unconditionally:

```sql
, 'effectivePeriod', jsonb_build_object(
      'start', ie_STARTTIME
    , 'end', ie_ENDTIME
  )
```

`Period.start`/`Period.end` are both `0..1`, so a zero-length administration is
still expressible, and a period whose start equals its end is valid FHIR. This
matches the documented mapping exactly and is a smaller change than the
chartevents fix.

If preserving `effectiveDateTime` for instantaneous administrations is wanted,
condition it on `starttime = endtime` rather than on `rate`:

```sql
, 'effectivePeriod',
    CASE WHEN ie_STARTTIME IS DISTINCT FROM ie_ENDTIME THEN
        jsonb_build_object('start', ie_STARTTIME, 'end', ie_ENDTIME)
    ELSE NULL END
, 'effectiveDateTime',
    CASE WHEN ie_STARTTIME IS NOT DISTINCT FROM ie_ENDTIME THEN ie_ENDTIME
    ELSE NULL END
```

That predicate is the one that actually determines whether an interval exists,
and on this data it moves 4,158,841 rows into `effectivePeriod` and leaves 1 as
an instant.

## Before filing

- [ ] Check the profile: does `SD_MimicMedicationAdministrationICU.fsh`
      constrain `effective[x]`? If it pins `effectiveDateTime`, the fix needs an
      IG change too. (The chartevents profile turned out to permit its fix; this
      one is unchecked.)
- [ ] Confirm no upstream issue already covers it — a search of
      `kind-lab/mimic-fhir` for `chartevents`/`valuenum`/`value` did not surface
      one, but I did not search for `effectivePeriod` or `inputevents`.
- [ ] Decide whether to file separately or bundle. Recommendation: **separately**
      — different resource, different reviewer, and the chartevents issue is
      already long.
- [ ] Consider whether `ingredientevents` (same source shape) has the same
      defect. Not checked.

## Reproduction

```sql
SELECT count(*)                                             AS rows_without_rate,
       count(*) FILTER (WHERE starttime IS DISTINCT FROM endtime) AS losing_an_interval,
       count(*) FILTER (WHERE date_diff('minute', starttime, endtime) > 5) AS over_5_min,
       quantile_cont(date_diff('second', starttime, endtime) / 60.0,
                     [0.5, 0.9, 0.99, 1.0])                 AS minutes_pctile
FROM read_csv('mimic-iv-2.2/icu/inputevents.csv.gz', header = true)
WHERE rate IS NULL;
```
