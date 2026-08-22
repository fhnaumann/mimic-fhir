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
  synthetic birthday's month and day are copied from an unrelated transfer
  event.

Both are fixed by one line. I found this while porting the official
`mimic-code` derived concepts onto MIMIC-on-FHIR and hitting a concept I could
not reproduce.

**Relates to #117 ("Anchor Years"), open since July 2024.** That issue reports
that `anchor_year` is missing from the published data; this one shows what the
omission costs, and that the value was not merely dropped but silently
substituted. The fix proposed below closes both.

The derivation is documented in the PhysioNet release notes, and I take the
choice of `transfers.intime` to have been deliberate — but the reason given
there is coverage of ED-only stays, weighed against `admissions.admittime`. The
anchor pair is a third option that beats both on exactly that criterion, and it
is the one MIMIC's own age definition uses. That comparison is in
[The published rationale](#the-published-rationale-and-why-the-fix-satisfies-it-better)
below.

## The mechanism

MIMIC-IV publishes no date of birth. It publishes an anchor pair on
`mimiciv_hosp.patients` — `anchor_age`, the patient's age in `anchor_year` —
and `mimic-code` defines age from it, in
[`mimic-iv/concepts/demographics/age.sql:30`](https://github.com/MIT-LCP/mimic-code/blob/8bcbd190ca75670cd5281f9ead3611ae1cefb73e/mimic-iv/concepts/demographics/age.sql#L30):

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

`anchor_age` is *mostly* recoverable — it is
`year(MIN(transfers.intime)) − year(birthDate)`, and the transfer history is
published as `Encounter.location[].period.start`
(`sql/fhir_encounter.sql:7-25`). Not exactly, though: that array is built
`GROUP BY hadm_id` from rows with `careunit IS NOT NULL`, so transfers carrying
no `hadm_id` — ED-only stays, the very rows the birth date rationale was chosen
to cover — attach to no hospital `Encounter`, and a consumer's earliest
published location can therefore postdate the `MIN(tfs.intime)` the ETL
actually used. On the 100-patient demo build that gap changes the recovered
year for 2 patients.

`anchor_year` does not survive in any form. The string `anchor_year` appears
nowhere in `sql/` at all — only `anchor_year_group`, once, in the demo cohort
filter at `fhir_etl/subjects.sql:16`. It is carried by no element and no
extension, and is not a function of any published timestamp.

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

This half of the problem is already on the tracker: **#117, "Anchor Years"**
("anchor years missing in data"), open since July 2024 and unanswered. That
issue reports the absence; this one measures what the absence costs and shows
that the value was silently substituted rather than merely omitted. The fix
below closes both — once `birthDate` reproduces the anchor pair exactly, a
consumer holding the resource can evaluate (1) without needing `anchor_year`
itself — so the two are worth resolving together rather than separately.

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
| …of those, with ≥1 hospital admission (the scored population) | 180,733 |
| admissions scored | 431,231 |
| **calendar-year age wrong** | **460 (0.107%)** |
| …patients affected | 87 (0.048% of the scored population) |
| …error range, years | −3 … **+13** |
| **elapsed-time age wrong** | **118,225 (27.42%)** |
| …patients affected | 47,899 (26.5% of the scored population) |

(Patient-level rates are quoted against the 180,733 patients who have an
admission to score. The other 118,979 `Patient` resources carry the same
defective birth date; they simply have no `admissions` row to measure it
against here.)

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
| [`comorbidity/charlson.sql:353-357`](https://github.com/MIT-LCP/mimic-code/blob/8bcbd190ca75670cd5281f9ead3611ae1cefb73e/mimic-iv/concepts/comorbidity/charlson.sql#L353-L357) | age bands 50/60/70/80 → `age_score`, added into `charlson_comorbidity_index` | **61** (31 patients) |
| [`score/sapsii.sql:373-379`](https://github.com/MIT-LCP/mimic-code/blob/8bcbd190ca75670cd5281f9ead3611ae1cefb73e/mimic-iv/concepts/score/sapsii.sql#L373-L379) | age bands 40/60/70/75/80 → `age_score`, added into `sapsii` and `sapsii_prob` | **69** (34 patients) |
| [`score/oasis.sql:144-150`](https://github.com/MIT-LCP/mimic-code/blob/8bcbd190ca75670cd5281f9ead3611ae1cefb73e/mimic-iv/concepts/score/oasis.sql#L144-L150) | age bands 24/53/77/89/90 → `age_score`, added into `oasis` and `oasis_prob` | **39** (28 patients) |
| [`measurement/creatinine_baseline.sql:12-26`](https://github.com/MIT-LCP/mimic-code/blob/8bcbd190ca75670cd5281f9ead3611ae1cefb73e/mimic-iv/concepts/measurement/creatinine_baseline.sql#L12-L26) | `age` enters `mdrd_est` continuously | **460** `mdrd_est` rows |

(Links are permalinked to `mimic-code` `8bcbd19`, current `main` at the time of
writing, so the line ranges stay valid.)

The banded scores absorb most of the error — only ages that cross a band
boundary move — but `creatinine_baseline` has no bands: `age` is a continuous
term in the MDRD estimate, so *every* one of the 460 wrong ages is a wrong
baseline creatinine. For `charlson` the wrong `age_score` is added directly
into the Charlson Comorbidity Index, so a published severity score is off by a
point for patients a consumer cannot identify.

(The `WHERE age >= 18` filter on `creatinine_baseline` does not in fact flip
any rows — 0 added, 0 dropped — because the affected patients are all well
clear of 18. It could, on other data.)

## The published rationale, and why the fix satisfies it better

The choice is documented, so let me address it directly. The
[MIMIC-IV on FHIR v2.1 release notes](https://physionet.org/content/mimic-iv-fhir/2.1/)
state, under `MimicPatient`:

> The intime column of the transfers table was used as a base for the birthdate
> calculation versus using the admittime column of the admissions table, since
> the admittime column is only available for hospitalized patients (i.e. it
> excludes ED only stays).

That is a deliberate decision, and on its own terms a correct one — but the
question it settles is *which event timestamp to anchor to*, and the criterion
it uses is **coverage**. Both candidates it weighs are event timestamps; the
anchor pair is not among them, and `anchor_year` is not mentioned anywhere in
the release notes.

The proposed fix wins on that same criterion. `anchor_year` sits on
`mimiciv_hosp.patients` — one row per patient, populated for every patient,
including ED-only ones. It has strictly better coverage than either timestamp
considered, and it needs no join at all: today `fhir_patient.sql` `INNER JOIN`s
`transfers` to build the birth date, so a patient with no transfer row silently
gets no `birthDate`. Anchoring to the patient row removes that dependency.

`anchor_year` is also already in scope where the birth date is built: the CTE
reads `mimiciv_hosp.patients` and groups by `pat.anchor_age` at
`fhir_patient.sql:33`; `pat.anchor_year` sits in the same row and is simply not
referenced. Nothing about FHIR, the profile, or the source schema forced the
substitution.

`git log -L15,15:sql/fhir_patient.sql` returns three commits — `63a7429`
(2021-12-06, the file's first version), `8e9344e` (2021-12-15, reformatting)
and `f907206` (2022-01-05, which only rewrote `::DATE` casts as
`CAST(... AS DATE)`). The expression's semantics have never been revisited.
This reads as an early choice made before the `mimic-code` concept library was
ported against it, rather than one that was weighed against the anchor pair and
rejected.

Nor is the value's absence news to consumers: #117 asked for it two years ago.

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

The second failure needs one more query. This one computes age the way a date
library does — completed years by the anniversary rule — and compares it to the
same canonical age:

```sql
-- tier 2: the elapsed-time age a conventional consumer computes
WITH t AS (
    SELECT subject_id, MIN(intime) AS min_intime
    FROM read_csv('mimic-iv-2.2/hosp/transfers.csv.gz', header = true)
    GROUP BY subject_id
), pp AS (
    SELECT p.subject_id, p.anchor_age, p.anchor_year
         , CAST(CAST(t.min_intime AS DATE)
                - CAST(p.anchor_age || ' years' AS INTERVAL) AS DATE) AS birth_date
    FROM read_csv('mimic-iv-2.2/hosp/patients.csv.gz', header = true) p
    JOIN t USING (subject_id)
), scored AS (
    SELECT
        YEAR(a.admittime) - YEAR(pp.birth_date)
          - CASE WHEN (MONTH(a.admittime), DAY(a.admittime))
                     < (MONTH(pp.birth_date), DAY(pp.birth_date))
                 THEN 1 ELSE 0 END                            AS age_elapsed
      , pp.anchor_age + (YEAR(a.admittime) - pp.anchor_year)  AS age_canonical
      , a.subject_id
    FROM read_csv('mimic-iv-2.2/hosp/admissions.csv.gz', header = true) a
    JOIN pp USING (subject_id)
)
SELECT count(*)                                                   AS admissions
     , count(*) FILTER (WHERE age_elapsed <> age_canonical)       AS wrong
     , count(DISTINCT subject_id)
         FILTER (WHERE age_elapsed <> age_canonical)              AS patients
FROM scored;
```

**118,225 wrong of 431,231, across 47,899 patients.** Swap `birth_date` for the
fixed expression (`MAKE_DATE(anchor_year, 1, 1) - anchor_age years`) in both
queries and the first returns no rows, the second reports `wrong = 0`.

Note the anniversary rule rather than `floor(days / 365.2425)`: the latter
accumulates enough drift to undershoot by a year at high ages against a
1 January birth date, which will make a correct fix look like it leaves a
residual. That bit me on a first run before I switched to the rule above.

The full script — the identity sweep, the six-candidate `anchor_year` recovery
sweep, the cross-check against `mimiciv_derived.age` and the downstream concept
projections — is a single file whose only real dependency is `duckdb`. The two
queries here cover both headline figures without it, but I am happy to provide
it or open it as a PR alongside the fix.

## Suggested fix

**Anchor the birth date to the anchor pair.** One line, and `anchor_year` is
already in the query:

```sql
-- sql/fhir_patient.sql:15
- , CAST(CAST(MIN(tfs.intime) AS DATE) - CAST(pat.anchor_age || 'years' AS INTERVAL) AS DATE) AS pat_BIRTH_DATE
+ , CAST(MAKE_DATE(pat.anchor_year, 1, 1) - CAST(pat.anchor_age || 'years' AS INTERVAL) AS DATE) AS pat_BIRTH_DATE
```

adding `pat.anchor_year` to the `GROUP BY` at line 33.

Scope of the change: it **replaces the value of an existing element**. No new
column in `mimic_fhir.patient` (still `id` + `fhir`), no new element or
extension in the resource, no profile or terminology change —
`Patient.birthDate` keeps its type, precision and cardinality, and
`pat.anchor_year` enters only as a grouping key in the CTE. The cost is that
the values differ from those already released on PhysioNet, so it lands with a
data re-release rather than silently.

Verified on full MIMIC-IV v2.2: after this change, ages computed from
`Patient.birthDate` match `mimiciv_derived.age` exactly — **0 / 431,231
admissions wrong under calendar-year subtraction, and 0 / 431,231 wrong under
the elapsed-time computation.**

The 1 January date is doing real work, not just standing in for an unknown day.
With a January 1 birthday the calendar-year and elapsed-time computations
coincide for every event date, so the two consumers described above can no
longer disagree. Choosing any other day would fix tier 1 and leave tier 2 in
place.

To be explicit about what "fixing" tier 2 can and cannot mean: **no true month
and day exists anywhere in MIMIC-IV to restore.** Deidentification destroyed
it, and for patients over 89 in their anchor year even the age is a sentinel
(`anchor_age = 91`). So the month and day cannot be made *correct*; they can
only be made *non-misleading*. Today they carry the month and day of an
unrelated transfer, which looks like information and is not. Under the fix they
carry the one convention that makes both standard age computations agree with
MIMIC's own definition — the same convention OMOP uses when only a birth year
is known.

The honest alternative is a year-only `birthDate`. FHIR `date` permits `YYYY`
precision, so `"birthDate": "2118"` would state exactly what is known and no
more. I did not propose it because it changes the element's precision for every
existing consumer and leaves elapsed-time age undefined rather than merely
consistent — but if the maintainers prefer declared imprecision over a
convention, it is a defensible choice and it also resolves tier 2. What is not
defensible is the status quo, where full day precision is published for a day
that means nothing.

Both birth dates are synthetic — MIMIC has no real one to publish. The
difference is that this one reproduces MIMIC's own definition of age exactly,
and the current one reproduces no defined quantity at all.

### One thing to decide

In principle, for a patient whose earliest event precedes their anchor year by
more than `anchor_age`, an anchor-derived birth date could postdate that event.
On MIMIC-IV v2.2 this does not happen: **0 patients**, and 0 admissions have a
negative canonical age. Worth knowing the case exists rather than discovering
it on a future release.

Happy to open a PR for the fix.

---

*Related, and from the same porting effort: `fhir_encounter.sql:65` casts
`admittime` through `TIMESTAMPTZ`, which normalises wall-clock times landing in
the US daylight-saving spring-forward gap and shifts them forward by an hour
unrecoverably (44 admissions on full MIMIC-IV v2.2). Filed separately as #124.*
