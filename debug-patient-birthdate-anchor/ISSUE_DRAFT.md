## Summary

`Patient.birthDate` is synthesised from the patient's earliest **transfer**
rather than from the MIMIC-IV **anchor pair**, so any age computed from the
published resource disagrees with the age MIMIC itself defines. Because neither
`anchor_age` nor `anchor_year` is published in any element or extension, a
consumer can neither correct the value nor determine which patients are
affected.

There are two consequences, one narrow and unrecoverable, one broad:

- On **460 of 431,231 admissions** (87 patients), calendar-year age from the
  published `birthDate` is wrong by whole years — by as much as **13**.
- On **118,225 of 431,231 admissions (27.4%)**, a consumer computing a
  conventional elapsed-time age gets a different answer again, because the
  synthetic birthday's month and day are copied from an unrelated admission
  event.

Both are fixed by one line. I found this while porting the official
`mimic-code` derived concepts onto MIMIC-on-FHIR and hitting a concept I could
not reproduce.

## The mechanism

MIMIC-IV publishes no date of birth. It publishes an anchor pair on
`mimiciv_hosp.patients` — `anchor_age`, the patient's age in `anchor_year` —
and `mimic-code` defines age from it, in
`mimic-iv/concepts/demographics/age.sql:30`:

```sql
pa.anchor_age + DATETIME_DIFF(ad.admittime, DATETIME(pa.anchor_year, 1, 1, 0, 0, 0), YEAR) AS age
```

`DATETIME_DIFF(..., YEAR)` counts calendar-year boundaries, so

```
age_canonical = anchor_age + year(admittime) − anchor_year            (1)
```

The ETL uses a different anchor — `sql/fhir_patient.sql:14-15`:

```sql
-- Set a birth date based on first hospital time, since MIMIC no longer provides a birth date
, CAST(CAST(MIN(tfs.intime) AS DATE) - CAST(pat.anchor_age || 'years' AS INTERVAL) AS DATE) AS pat_BIRTH_DATE
```

written to the resource at `sql/fhir_patient.sql:108`. Subtracting whole years
from a date preserves month and day, so `year(birthDate) = year(MIN(transfers.intime)) − anchor_age`,
and a consumer performing the same calendar-year subtraction the canonical SQL
performs gets

```
age_published = year(admittime) − year(birthDate)
              = anchor_age + year(admittime) − year(MIN(transfers.intime))   (2)
```

Subtracting (1) from (2), every term cancels except the anchor:

> **`age_published − age_canonical = year(MIN(transfers.intime)) − anchor_year`**

The two agree only where a patient's earliest transfer happens to fall in their
anchor year. Where it does not, the published age is wrong by exactly that gap,
and the resource records nothing that would let a consumer notice.

## Why this cannot be repaired downstream

`anchor_age` survives — it is `year(MIN(transfers.intime)) − year(birthDate)`,
and the transfer history is published as `Encounter.location[].period.start`
(`sql/fhir_encounter.sql:11-25`). `anchor_year` does not survive in any form.
It appears nowhere in `sql/` outside `fhir_patient.sql:15,33`, is carried by no
element and no extension, and is not a function of any published timestamp.

I swept the six candidate identities a consumer could build from the timestamps
MIMIC-on-FHIR does publish. On full MIMIC-IV v2.2:

| candidate for `anchor_year` | exact | |
|---|---|---|
| `year(MIN(transfers.intime))` — what the ETL implicitly assumes | 299,625 / 299,712 (99.971%) | not an identity |
| `year(MAX(transfers.intime))` | 202,110 / 299,712 (67.43%) | not an identity |
| `year(MIN(admissions.admittime))` — `Encounter.period.start` | 161,316 / 299,712 (53.82%) | not an identity |
| `year(MIN(admissions.dischtime))` | 159,418 / 299,712 (53.19%) | not an identity |
| `year(MAX(admissions.admittime))` | 111,885 / 299,712 (37.33%) | not an identity |
| `year(MAX(admissions.dischtime))` | 110,428 / 299,712 (36.84%) | not an identity |

The intuitive guess — that `anchor_year` is "the year the patient first
appears" — is false. One subject in the full dataset has an admission two years before their
anchor year.
`anchor_year` is a deidentification artefact with no event-derived definition,
which is exactly why it has to be carried rather than reconstructed.

So the failure is not a lossy field that a consumer can route around. **The
affected patients are unmarked**: singling them out requires the very value
that is missing. The great majority of published ages are right — but which
ones cannot be determined from the published data, so none of them can be
relied on individually.

## Worked example

One subject in the full dataset (identifier withheld) has an earliest transfer
nine years before their anchor year.

| | |
|---|---|
| published `birthDate` | `MIN(transfers.intime) − anchor_age` |
| anchor-pair birth date | `Jan 1 of anchor_year − anchor_age` |

Every admission for this patient is published nine years old, and nothing in
the `Patient` resource — or any other resource — distinguishes them from a
patient whose birth date is right.

The worst case is a subject whose first transfer precedes their anchor year by
13 years; their admission is published **13 years** too old.

## Scope on full MIMIC-IV v2.2

| fact | count |
|---|---|
| patients given a `Patient` resource | 299,712 |
| admissions scored | 431,231 |
| **calendar-year age wrong** | **460 (0.107%)** |
| …patients affected | 87 |
| …error range, years | −3 … **+13** |
| **elapsed-time age wrong** | **118,225 (27.42%)** |
| …patients affected | 47,899 |

The error is per-patient and repeats across every one of that patient's stays,
which is why 87 patients account for 460 admissions. It skews strongly positive
— 454 of the 460 publish an age that is too *high*:

| error | admissions | patients |
|---|---|---|
| −3 | 1 | 1 |
| −1 | 5 | 1 |
| +1 | 288 | 44 |
| +2 | 108 | 19 |
| +3 | 23 | 7 |
| +4 | 5 | 3 |
| +5 | 5 | 1 |
| +6 | 12 | 4 |
| +7 | 4 | 2 |
| +9 | 4 | 1 |
| +12 | 4 | 3 |
| +13 | 1 | 1 |

### The second failure: the month and day are meaningless

Even for the majority of patients where the *year* comes out right, the
published birth date's month and day are copied from whenever that patient
first happened to be transferred. Two consumers computing age from the same
resource get different answers:

- calendar-year subtraction (what `mimic-code` does) → correct for those
  patients;
- completed years since the birth date — the anniversary rule every date
  library implements, and what most FHIR tooling will do → **off by a year on
  118,225 admissions (27.42%), affecting 47,899 patients**.

A `Patient.birthDate` that yields a different age depending on which of the two
standard computations you use is not carrying an age at all. This is invisible
to anyone who does not already know how MIMIC defines age, and it affects every
patient rather than the tier 1 minority.

## Impact on the official `mimic-code` concepts

Four concepts read `mimiciv_derived.age`:

| concept | how `age` is used | admissions whose output changes |
|---|---|---|
| `comorbidity/charlson.sql:353-357` | age bands 50/60/70/80 → `age_score`, added into `charlson_comorbidity_index` | **61** (31 patients) |
| `score/sapsii.sql:373-379` | age bands 40/60/70/75/80 → `age_score`, added into `sapsii` and `sapsii_prob` | **69** (34 patients) |
| `score/oasis.sql:144-150` | age bands 24/53/77/89/90 → `age_score`, added into `oasis` and `oasis_prob` | **39** (28 patients) |
| `measurement/creatinine_baseline.sql:12-26` | `age` enters `mdrd_est` continuously | **460** `mdrd_est` rows |

The banded scores absorb most of the error — only ages that cross a band
boundary move — but `creatinine_baseline` has no bands: `age` is a continuous
term in the MDRD estimate, so *every* one of the 460 wrong ages is a wrong
baseline creatinine. For `charlson` the wrong `age_score` is added directly
into the Charlson Comorbidity Index, so a published severity score is off by a
point for patients a consumer cannot identify.

(The `WHERE age >= 18` filter on `creatinine_baseline` does not in fact flip
any rows — 0 added, 0 dropped — because the affected patients are all well
clear of 18. It could, on other data.)

## This is not a modelling constraint — the value was available

`anchor_year` is already in scope where the birth date is built. The CTE reads
`mimiciv_hosp.patients` and groups by `pat.anchor_age` at `fhir_patient.sql:33`;
`pat.anchor_year` sits in the same row and is simply not referenced. Nothing
about FHIR, the profile, or the source schema forced the substitution.

`git log -L15,15:sql/fhir_patient.sql` returns two commits — `63a7429`
(December 2021, the file's first version) and `f907206` (January 2022), which
only rewrote `::DATE` casts as `CAST(... AS DATE)`. The expression has never
been revisited on its merits. This reads as an early choice that predates the
`mimic-code` concept library being ported against it, rather than a decision
anyone would make again.

## Reproduction

Everything above comes from the PhysioNet MIMIC-IV v2.2 distribution and needs
nothing but DuckDB. The published side is derived from the generation SQL
rather than read out of a warehouse, so the check is independent of how any
particular consumer loaded the release.

```sql
-- the identity: published age minus canonical age is exactly the anchor gap
WITH t AS (
    SELECT subject_id, MIN(intime) AS min_intime
    FROM read_csv('mimic-iv-2.2/hosp/transfers.csv.gz', header = true)
    GROUP BY subject_id
), pp AS (
    SELECT p.subject_id, p.anchor_age, p.anchor_year, t.min_intime
         -- sql/fhir_patient.sql:15, transcribed
         , CAST(CAST(t.min_intime AS DATE)
                - CAST(p.anchor_age || ' years' AS INTERVAL) AS DATE) AS birth_date
    FROM read_csv('mimic-iv-2.2/hosp/patients.csv.gz', header = true) p
    JOIN t USING (subject_id)
)
SELECT
    YEAR(a.admittime) - YEAR(pp.birth_date)                     AS age_published
  , pp.anchor_age + (YEAR(a.admittime) - pp.anchor_year)        AS age_canonical
  , count(*)                                                     AS admissions
FROM read_csv('mimic-iv-2.2/hosp/admissions.csv.gz', header = true) a
JOIN pp USING (subject_id)
GROUP BY 1, 2
HAVING age_published <> age_canonical
ORDER BY admissions DESC;

-- and the sanity check that the canonical formula is transcribed correctly:
-- this must return 0 against mimiciv_derived.age.
```

That query returns **460 admissions across 87 patients**. I ran the canonical
side against `mimiciv_derived.age` first and it agrees on all 431,231 rows, so
the disagreement above is the ETL's, not a mis-transcribed formula.

The full script — the identity sweep, the elapsed-time comparison and the
downstream projections — is a single file whose only real dependency is
`duckdb`, and I am happy to attach it or open it as a PR alongside the fix.

## Suggested fix

**Anchor the birth date to the anchor pair.** One line, and `anchor_year` is
already in the query:

```sql
-- sql/fhir_patient.sql:15
- , CAST(CAST(MIN(tfs.intime) AS DATE) - CAST(pat.anchor_age || 'years' AS INTERVAL) AS DATE) AS pat_BIRTH_DATE
+ , CAST(MAKE_DATE(pat.anchor_year, 1, 1) - CAST(pat.anchor_age || 'years' AS INTERVAL) AS DATE) AS pat_BIRTH_DATE
```

adding `pat.anchor_year` to the `GROUP BY` at line 33.

Verified on full MIMIC-IV v2.2: after this change, ages computed from
`Patient.birthDate` match `mimiciv_derived.age` exactly — **0 / 431,231
admissions wrong under calendar-year subtraction, and 0 / 431,231 wrong under
the elapsed-time computation.**

The 1 January date is doing real work, not just standing in for an unknown day.
With a January 1 birthday the calendar-year and elapsed-time computations
coincide for every event date, so the two consumers described above can no
longer disagree. Choosing any other day would fix tier 1 and leave tier 2 in
place.

Both birth dates are synthetic — MIMIC has no real one to publish. The
difference is that this one reproduces MIMIC's own definition of age exactly,
and the current one reproduces no defined quantity at all.

### Alternative, if changing published values is unwelcome

Changing `birthDate` changes data in already-released resources, which may not
be acceptable. Carrying the anchor pair as an extension on `Patient` would also
resolve the unrecoverability — a consumer could then evaluate (1) directly:

```jsonc
"extension": [{
  "url": "http://mimic.mit.edu/fhir/mimic/StructureDefinition/patient-anchor",
  "extension": [
    { "url": "anchorAge",  "valueInteger": 91 },
    { "url": "anchorYear", "valueInteger": 2165 }
  ]
}]
```

This is purely additive and invisible to existing consumers. It does leave the
published `birthDate` misleading for the elapsed-time case, so my preference is
the one-line fix, or both.

I would also accept `anchor_year_group` being dropped from any such extension —
it is a real-world calendar bucket and date shifting is precisely the operation
that destroys the mapping to it, so unlike the anchor pair it genuinely cannot
be represented.

### One thing to decide

In principle, for a patient whose earliest event precedes their anchor year by
more than `anchor_age`, an anchor-derived birth date could postdate that event.
On MIMIC-IV v2.2 this does not happen: **0 patients**, and 0 admissions have a
negative canonical age. Worth knowing the case exists rather than discovering
it on a future release.

Happy to open a PR for whichever direction is preferred.

---

*Related, and from the same porting effort: `fhir_encounter.sql:65` casts
`admittime` through `TIMESTAMPTZ`, which normalises wall-clock times landing in
the US daylight-saving spring-forward gap and shifts them forward by an hour
unrecoverably (44 admissions on full MIMIC-IV v2.2). Filed separately as #124.*
