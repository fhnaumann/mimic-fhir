# `Patient.birthDate` is derived from the wrong anchor

Working document. Mechanism, method, results, and what is still open.
The report to file upstream is `ISSUE_DRAFT.md`.

## Mechanism

MIMIC-IV does not publish a date of birth. It publishes an **anchor pair** on
`mimiciv_hosp.patients` — `anchor_age` and `anchor_year` — and defines age as
the age in `anchor_year` plus the years elapsed since. `mimic-code`'s own
`mimic-iv/concepts/demographics/age.sql:30`:

```sql
pa.anchor_age + DATETIME_DIFF(ad.admittime, DATETIME(pa.anchor_year, 1, 1, 0, 0, 0), YEAR) AS age
```

`DATETIME_DIFF(..., YEAR)` counts calendar-year boundaries, so this is

    age_canonical = anchor_age + year(admittime) − anchor_year                (1)

The FHIR ETL instead synthesises a birth date from the patient's earliest
transfer — `sql/fhir_patient.sql:14-15`:

```sql
-- Set a birth date based on first hospital time, since MIMIC no longer provides a birth date
, CAST(CAST(MIN(tfs.intime) AS DATE) - CAST(pat.anchor_age || 'years' AS INTERVAL) AS DATE) AS pat_BIRTH_DATE
```

written to the resource at `sql/fhir_patient.sql:108`. Subtracting a whole
number of years from a date preserves month and day, so

    year(birthDate) = year(MIN(transfers.intime)) − anchor_age                (2)

A consumer computing age the way MIMIC does — calendar-year subtraction —
therefore gets

    age_published = year(admittime) − year(birthDate)
                  = anchor_age + year(admittime) − year(MIN(transfers.intime)) (3)

Subtracting (1) from (3), everything cancels but the anchor:

    **age_published − age_canonical = year(MIN(transfers.intime)) − anchor_year**

So the two agree **iff the patient's earliest transfer falls in their anchor
year**, and where they disagree the error is exactly the gap between those two
years. `anchor_year` never appears in the published resource, so the ETL has
substituted one quantity for another without recording the difference.

## Two distinct failures

**Tier 1 — the wrong year.** Where `year(MIN(transfers.intime)) ≠ anchor_year`,
the published age is wrong by whole years. Rare but unbounded, and it is the
one that cannot be worked around: recovering the true age needs `anchor_year`.

**Tier 2 — the wrong month and day.** The published birth date inherits its
month and day from an unrelated admission event. A consumer computing a
conventional elapsed-time age — completed years, the anniversary rule every
date library implements — rather than a calendar-year difference lands on a
*third* number, off by a year on 27.42% of admissions, because the synthetic
birthday falls mid-year. This affects patients right across the cohort, not
just the tier 1 minority.

Both are fixed by the same one-line change (see below). Tier 2 is the larger
number; tier 1 is the one that makes the data unrecoverable.

## Why it cannot be repaired downstream

`anchor_age` *is* recoverable — it is `year(MIN(transfers.intime)) − year(birthDate)`,
and the transfer history is published as `Encounter.location[].period.start`
(`sql/fhir_encounter.sql:11-25`). `anchor_year` is not. It appears nowhere in
`sql/` outside `fhir_patient.sql:15,33` and `fhir_etl/subjects.sql:16`, is
carried by no element and no extension, and — critically — is **not a function
of any published timestamp**. The script sweeps six candidate identities built
from the timestamps MIMIC-on-FHIR does publish (first/last transfer, first/last
admission, first/last discharge); none is exact on full data.

The consequence is worse than a lossy field. A consumer cannot identify *which*
patients are affected, because doing so requires the value that is missing. The
published ages are wrong on an unmarked subset.

### Ruling out the obvious guess

`anchor_year` is not "the year the patient first appears". Counter-example from
the full-data port run (`concepts_fhir/concepts/demographics/age/attempt_0002/comparison.full.json`):
one subject has an admission two years *before* their anchor year. It is a
deidentification artefact with no event-derived definition, which is precisely
why it has to be carried rather than reconstructed.

## Method

`hpc/patient_birthdate_anchor.py`, DuckDB over the MIMIC-IV v2.2 PhysioNet
distribution.

1. Reproduce the published birth date by transcribing `fhir_patient.sql:11-32`,
   including its `INNER JOIN` on `transfers` (patients with no transfer row get
   no `Patient` resource, so they are out of scope here too).
2. Reproduce the canonical age by transcribing `age.sql:30`.
3. **Assert (2) matches `mimiciv_derived.age` row-for-row** in the prebuilt
   oracle. The script aborts if it does not — a mis-transcribed formula would
   manufacture a convincing bug that is not there.
4. Compare canonical against both consumer-side computations (tier 1, tier 2).
5. Sweep the six `anchor_year` recovery candidates.
6. Project the error onto the four official concepts that read
   `mimiciv_derived.age`, using their own age bands.
7. Re-run 4 under the proposed fix, which should return zero on both.

Nothing is read from our Delta/Pathling warehouse. The oracle is opened
`READ_ONLY` and used only for step 3.

## Results

### Demo build (100 patients, 275 admissions) — smoke run

    cross-check vs mimiciv_derived.age : 0 mismatches / 275   ✓

    tier 1 (calendar-year age)  :   0 / 275   (0.00%)
    tier 2 (elapsed-time age)   : 101 / 275   (36.73%), 30 patients, error −1..0
    after the proposed fix      :   0 wrong on both computations

    anchor_year recovery candidates
      year(MIN(transfers.intime))       100/100  (100.00%)   <- holds *on demo only*
      year(MIN(admissions.admittime))    98/100  (98.00%)
      year(MIN(admissions.dischtime))    95/100  (95.00%)
      year(MAX(admissions.admittime))    65/100  (65.00%)
      year(MAX(transfers.intime))        63/100  (63.00%)
      year(MAX(admissions.dischtime))    64/100  (64.00%)

**The demo cannot show tier 1.** All 100 demo patients satisfy
`year(MIN(transfers.intime)) == anchor_year`, so the defect measures zero there
while measuring 460 admissions on full data. This is not a curiosity — a
demo-based probe during the concept port concluded the anchor relation held and
that conclusion survived until the full run contradicted it. Any figure in the
issue must come from the full cohort.

The demo also misstates the numbers it *can* show: tier 2 reads 36.73% here
against 27.42% on full data, and `year(MIN(admissions.admittime))` matches 98
of 100 patients here against 53.82% on full data. Treat this run as a test of
the script, nothing more.

### Full cohort — `hpc/results/job-29971523/`

299,712 patients, 431,231 admissions, MIMIC-IV v2.2 CSV distribution.

    cross-check vs mimiciv_derived.age : 0 mismatches / 431,231   ✓

    tier 1 (calendar-year age)  :     460 / 431,231  (0.1067%),  87 patients, error −3..+13
    tier 2 (elapsed-time age)   : 118,225 / 431,231 (27.4157%), 47,899 patients, error −3..+13
    after the proposed fix      :       0 wrong on both computations

    anchor_year recovery candidates — none is an identity
      year(MIN(transfers.intime))     299,625/299,712  (99.9710%)
      year(MAX(transfers.intime))     202,110/299,712  (67.4347%)
      year(MIN(admissions.admittime)) 161,316/299,712  (53.8237%)
      year(MIN(admissions.dischtime)) 159,418/299,712  (53.1904%)
      year(MAX(admissions.admittime)) 111,885/299,712  (37.3308%)
      year(MAX(admissions.dischtime)) 110,428/299,712  (36.8447%)

    downstream
      charlson             age_score changes on 61 admissions (31 patients)
      sapsii               age_score changes on 69 admissions (34 patients)
      oasis                age_score changes on 39 admissions (28 patients)
      creatinine_baseline  mdrd_est changes on 460 rows; cohort unchanged

**The tier 1 figure reproduces the concept port exactly** — 460 admissions,
derived here from the source distribution alone with no Delta, Pathling or
Spark involved. `charlson`'s 61 likewise matches its port run. That is the
result worth having: two independent routes to the same number.

Three things the port run did not show:

- The error reaches **+13 years**, not +6. Worst case is one subject whose published birth year is 13 years
  earlier than the anchor-pair birth year.
- Only **87 patients** are affected, but they account for 460 admissions —
  the error is per-patient and repeats across all of their stays.
- Errors are overwhelmingly positive (published age too high): 454 of the 460.
  <11 rows across <11 patients run negative.

Two draft claims the run corrected:

- **`creatinine_baseline`'s `age >= 18` filter never actually flips.** 0 rows
  added, 0 dropped. The affected patients are all well clear of 18. The `age`
  and `mdrd_est` errors are real (460 rows); the cohort-membership argument is
  not, and has been removed from the issue.
- **`year(MIN(admissions.admittime))` is not close.** On the demo it matched 98
  of 100 patients, which made "anchor_year is the first admission year" look
  worth ruling out carefully. On full data it matches 53.82% — it was never a
  contender. The demo misleads in both directions.

### An artefact caught in the first run

Job `29971161` reported **823 admissions still wrong under the elapsed-time
computation after the fix**, which would have been a real problem for the
recommendation. It was a defect in the measurement, not in the fix: that run
computed elapsed-time age as `floor(days / 365.2425)`, and for a 1 January
birth date at high ages the accumulated drift crosses an integer boundary and
undershoots by a year. Replaced with the anniversary rule every date library
actually implements —

    year(event) − year(birth) − (1 if (month, day) of event < (month, day) of birth)

— which returns 0 after the fix, as the algebra says it must. Job `29971523` is
the authoritative run; `29971161` is kept for provenance.

## The fix

One line. Anchor the birth date to the anchor pair instead of the first
transfer:

```sql
-- sql/fhir_patient.sql:15
- , CAST(CAST(MIN(tfs.intime) AS DATE) - CAST(pat.anchor_age || 'years' AS INTERVAL) AS DATE) AS pat_BIRTH_DATE
+ , CAST(MAKE_DATE(pat.anchor_year, 1, 1) - CAST(pat.anchor_age || 'years' AS INTERVAL) AS DATE) AS pat_BIRTH_DATE
```

`anchor_year` is already in scope — the CTE reads `mimiciv_hosp.patients` and
groups by `pat.anchor_age` at line 33; the fix adds `pat.anchor_year` to that
`GROUP BY`.

Verified on the full cohort (`job-29971523`, and on the demo build before it):
**0 / 431,231 wrong under calendar-year subtraction and 0 / 431,231 wrong under
the elapsed-time computation**, i.e. the fix closes tier 2 as well as tier 1. That second property is why January 1 is the right day rather than an
arbitrary one — with a 1 January birthday the two age computations coincide for
every event date, so a consumer cannot get a different answer by computing age
the ordinary way.

Both birth dates are synthetic. The difference is that this one reproduces
MIMIC's own definition of age exactly, and the current one reproduces nothing.

### Open questions for the maintainers

- Changing `birthDate` changes values in already-published resources, so the
  fix lands with a data re-release rather than silently. Stated in the issue as
  the cost of the fix, not offered as a reason to hesitate.
  (An earlier draft offered a second route — carry `anchor_age`/`anchor_year`
  as a `Patient` extension and leave `birthDate` alone. **Dropped from the
  report**: it fixes only tier 1, leaves the published `birthDate` just as
  misleading for tier 2, and is not a one-line change — it needs a
  `StructureDefinition` in `mimic-profiles`, wiring into
  `fhir_etl.fn_patient_extension`, and an IG regeneration, or validation
  reports the extension as unresolvable, as in upstream #97. Offering it
  alongside the real fix only invited the weaker option. Recorded here in case
  a maintainer raises it.)
- `fhir_patient.sql` reads `mimiciv_hosp.patients` directly, so a full build
  includes `anchor_age = 0` patients (`fhir_etl/subjects.sql` is the 100-subject
  *demo* cohort builder — note its `LIMIT 100` — not a filter on the full
  release). For a patient whose earliest event precedes their anchor year, the
  fixed birth date can therefore postdate that event. The script counts these.
  Note this is not an artefact of the fix: canonical `age.sql` already returns
  a negative age for exactly those rows, and the fix reproduces the oracle
  faithfully, oddities included. Worth reporting the count so the maintainers
  can decide whether to clamp.

## Pre-filing review (2026-08-17)

What was checked before the issue goes up. Everything here changed the draft.

**The derivation is documented and was deliberate — as to the timestamp, not
the anchor.** The [MIMIC-IV on FHIR v2.1 release
notes](https://physionet.org/content/mimic-iv-fhir/2.1/) say under
`MimicPatient`: *"The intime column of the transfers table was used as a base
for the birthdate calculation versus using the admittime column of the
admissions table, since the admittime column is only available for hospitalized
patients (i.e. it excludes ED only stays)."* So the maintainers weighed
`transfers.intime` against `admissions.admittime` on a **coverage** criterion
and picked correctly between them. `anchor_year` is not mentioned in the
release notes at all. The draft now leads with this rather than being ambushed
by it, and argues the fix wins on the maintainers' own criterion:
`anchor_year` is on the patient row, so it covers ED-only patients *and*
patients with no `transfers` row (whom the current `INNER JOIN` leaves with no
`birthDate` at all).

**Prior art upstream: #117 "Anchor Years" — "anchor years missing in data",
open since 2024-07-15, no replies.** Consumer-side report of the same missing
value. Now cross-referenced.

**`anchor_year` never appears in `sql/`.** The draft said it appeared at
`fhir_patient.sql:15,33` — those are `anchor_age`. Only `anchor_year_group`
occurs, once, at `fhir_etl/subjects.sql:16`. Corrected; the claim is stronger
than it was written.

**`anchor_age` is not cleanly recoverable either.** The draft asserted it
survives as `year(MIN(transfers.intime)) − year(birthDate)` via
`Encounter.location[].period.start`. But `transfer_locations` is built
`GROUP BY hadm_id` over `careunit IS NOT NULL`, so transfers with a NULL
`hadm_id` (ED-only) reach no hospital `Encounter`. Measured on the demo build:
the `careunit` filter never moves the per-patient minimum (the NULL-careunit
rows are all `eventtype = 'discharge'`, never earliest), but the missing
`hadm_id` rows move it for 4/100 patients and move the *year* for 2/100.
Softened accordingly. **Not yet measured on full data** — worth a column in the
next run if this is ever re-run.

**Tier 2 is now reproducible from the issue text alone.** The draft previously
carried a standalone query for tier 1 only, leaving its largest figure
(118,225 admissions) resting on "trust the script". Added a second self-contained
DuckDB query using the anniversary rule, plus a note on why *not* to use
`floor(days / 365.2425)` — the `29971161` artefact, stated up front so a
maintainer reproducing this does not rediscover it and conclude the fix leaves a
residual. Validated against the demo build before going in the draft: returns
101/275 (36.73%), 30 patients, error −1..0, matching the demo smoke run exactly.

Decided against attaching the script itself. The two inline queries now cover
both headline numbers with no dependencies, whereas the script's extra reach —
the oracle cross-check and the concept projections — needs a prebuilt
`mimiciv_derived` that a reader will not have, so attaching it mostly invites
review of the measurement instead of the defect. The offer stays in the issue;
it goes in the PR if they engage.

Also noted, for anyone reading the CSVs alongside the issue: `error_years` in
`affected_patients.csv` is `year_min_intime − anchor_year`, the negative of the
`published − canonical` convention used in `report.txt` and the issue. Same
magnitudes, opposite sign.

## Notes for whoever runs this

- The demo run is a smoke test of the *script*, not evidence. Tier 1 needs the
  full cohort.
- `--mimic-root` is authoritative; `--oracle-db` is only the cross-check. If
  the oracle is unavailable the script still runs but reports the check as
  skipped, and the results should be treated as unverified.
- The script also works with `--oracle-db` alone (no `--mimic-root`), reading
  source tables from the DuckDB. That is the laptop path and is how the demo
  numbers above were produced.
