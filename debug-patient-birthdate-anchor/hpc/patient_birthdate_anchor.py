#!/usr/bin/env python3
"""Quantify the MIMIC-on-FHIR `Patient.birthDate` anchor defect.

`sql/fhir_patient.sql:15` synthesises the published birth date as

    CAST(CAST(MIN(tfs.intime) AS DATE) - CAST(pat.anchor_age || 'years' AS INTERVAL) AS DATE)

i.e. the patient's *earliest transfer* minus `anchor_age`. MIMIC-IV defines age
through the anchor pair instead -- `mimic-iv/concepts/demographics/age.sql:30`:

    anchor_age + DATETIME_DIFF(admittime, DATETIME(anchor_year, 1, 1, 0, 0, 0), YEAR)

which is `anchor_age + year(admittime) - anchor_year`.

Substituting the published birth date into the natural FHIR-side age gives

    year(admittime) - year(birthDate) = anchor_age + year(admittime) - year(MIN(intime))

so the two agree **iff `year(MIN(transfers.intime)) == anchor_year`**, and the
residual error is exactly `year(MIN(transfers.intime)) - anchor_year` years.
Neither `anchor_age` nor `anchor_year` is published anywhere in MIMIC-on-FHIR,
so a consumer can neither correct the value nor detect which rows are wrong.

Separately, because the published birth date inherits its month and day from an
unrelated admission event, a consumer computing a conventional elapsed-time age
lands on a different number again -- for roughly half of all admissions.

Everything here is measured against the original MIMIC-IV v2.2 distribution.
The published side is reproduced from the ETL's own SQL rather than read out of
a warehouse, so the result is reproducible by anyone holding the PhysioNet
files and DuckDB, and depends on no artefact only we hold.

Usage
-----
    python patient_birthdate_anchor.py \
        --mimic-root /path/to/mimic-iv-2.2 \
        --oracle-db  /path/to/mimic4-full.db \
        --out-dir    ./results/job-1

`--mimic-root` is the authoritative source. `--oracle-db` is optional and used
only to cross-check: the canonical age formula reproduced here is asserted to
agree row-for-row with `mimiciv_derived.age` before any result is believed.

If `--mimic-root` is omitted, the source tables are read from `--oracle-db`
instead, which is how this runs on a laptop against the demo build.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

# Age bands of every official concept that reads `mimiciv_derived.age`.
#   comorbidity/charlson.sql:353-357     score/oasis.sql:144-150
#   score/sapsii.sql:373-379             measurement/creatinine_baseline.sql:12-26
CHARLSON_BAND = """
    CASE WHEN {a} <= 50 THEN 0
         WHEN {a} <= 60 THEN 1
         WHEN {a} <= 70 THEN 2
         WHEN {a} <= 80 THEN 3
         ELSE 4 END
"""
SAPSII_BAND = """
    CASE WHEN {a} IS NULL THEN NULL
         WHEN {a} < 40 THEN 0
         WHEN {a} < 60 THEN 7
         WHEN {a} < 70 THEN 12
         WHEN {a} < 75 THEN 15
         WHEN {a} < 80 THEN 16
         ELSE 18 END
"""
OASIS_BAND = """
    CASE WHEN {a} IS NULL THEN NULL
         WHEN {a} < 24 THEN 0
         WHEN {a} <= 53 THEN 3
         WHEN {a} <= 77 THEN 6
         WHEN {a} <= 89 THEN 9
         ELSE 7 END
"""


def build_sources(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> str:
    """Register `patients`, `admissions` and `transfers` as views.

    Returns a human-readable description of where they came from.
    """
    if args.mimic_root:
        root = Path(args.mimic_root)
        paths = {
            "patients": root / "hosp" / "patients.csv.gz",
            "admissions": root / "hosp" / "admissions.csv.gz",
            "transfers": root / "hosp" / "transfers.csv.gz",
        }
        missing = [str(p) for p in paths.values() if not p.exists()]
        if missing:
            sys.exit("missing source file(s):\n  " + "\n  ".join(missing))
        for name, path in paths.items():
            con.execute(
                f"CREATE VIEW {name} AS "
                f"SELECT * FROM read_csv('{path}', header = true)"
            )
        return f"MIMIC-IV v2.2 CSV distribution at {root}"

    if not args.oracle_db:
        sys.exit("one of --mimic-root or --oracle-db is required")

    con.execute(f"ATTACH '{args.oracle_db}' AS oracle (READ_ONLY)")
    for name in ("patients", "admissions", "transfers"):
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM oracle.mimiciv_hosp.{name}")
    return f"prebuilt DuckDB at {args.oracle_db} (mimiciv_hosp)"


def build_core(con: duckdb.DuckDBPyConnection) -> None:
    """The reproduction: published birth date, canonical age, FHIR-side ages."""

    # The ETL's own expression, transcribed from sql/fhir_patient.sql:11-32.
    # INNER JOIN on transfers, grouped by subject -- patients with no transfer
    # row get no Patient resource at all, exactly as upstream.
    con.execute(
        """
        CREATE TABLE published_patient AS
        SELECT
            p.subject_id
            , p.anchor_age
            , p.anchor_year
            , p.anchor_year_group
            , t.min_intime
            -- sql/fhir_patient.sql:15
            , CAST(CAST(t.min_intime AS DATE)
                   - CAST(p.anchor_age || ' years' AS INTERVAL) AS DATE)
              AS birth_date_published
            -- the one-line fix proposed in the issue
            , CAST(MAKE_DATE(p.anchor_year, 1, 1)
                   - CAST(p.anchor_age || ' years' AS INTERVAL) AS DATE)
              AS birth_date_fixed
        FROM patients p
        INNER JOIN (
            SELECT subject_id, MIN(intime) AS min_intime
            FROM transfers
            GROUP BY subject_id
        ) t ON t.subject_id = p.subject_id
        """
    )

    # One row per admission: the canonical age, and the two ages a FHIR
    # consumer can arrive at from the published birthDate.
    con.execute(
        """
        CREATE TABLE age_compare AS
        SELECT
            a.subject_id
            , a.hadm_id
            , a.admittime
            , pp.anchor_age
            , pp.anchor_year
            , pp.min_intime
            , pp.birth_date_published
            , pp.birth_date_fixed
            -- mimic-iv/concepts/demographics/age.sql:30
            , pp.anchor_age + (YEAR(a.admittime) - pp.anchor_year) AS age_canonical
            -- what a consumer gets from the published birthDate, calendar-year
            -- subtraction (the same operation the canonical SQL performs)
            , YEAR(a.admittime) - YEAR(pp.birth_date_published) AS age_yeardiff
            -- what a consumer gets computing a conventional elapsed-time age:
            -- completed years, i.e. how many birthdays have passed. This is
            -- the anniversary rule every date library implements; a
            -- days/365.2425 proxy is NOT equivalent -- it drifts by a year at
            -- high ages for 1 January birthdays, which would put a spurious
            -- residual on the fix below.
            , YEAR(a.admittime) - YEAR(pp.birth_date_published)
              - CASE WHEN MONTH(a.admittime) < MONTH(pp.birth_date_published)
                       OR (MONTH(a.admittime) = MONTH(pp.birth_date_published)
                           AND DAY(a.admittime) < DAY(pp.birth_date_published))
                     THEN 1 ELSE 0 END AS age_elapsed
            -- the same two computations under the proposed fix
            , YEAR(a.admittime) - YEAR(pp.birth_date_fixed) AS age_yeardiff_fixed
            , YEAR(a.admittime) - YEAR(pp.birth_date_fixed)
              - CASE WHEN MONTH(a.admittime) < MONTH(pp.birth_date_fixed)
                       OR (MONTH(a.admittime) = MONTH(pp.birth_date_fixed)
                           AND DAY(a.admittime) < DAY(pp.birth_date_fixed))
                     THEN 1 ELSE 0 END AS age_elapsed_fixed
        FROM admissions a
        INNER JOIN published_patient pp ON pp.subject_id = a.subject_id
        """
    )


def check_against_oracle(
    con: duckdb.DuckDBPyConnection, oracle_db: str | None, attached: bool
) -> dict:
    """Assert the reproduced canonical age matches `mimiciv_derived.age`.

    No number in this report is believed unless this passes: it is what rules
    out the whole result being an artefact of a mis-transcribed formula.
    """
    if not oracle_db:
        return {"ran": False, "reason": "no --oracle-db supplied"}

    if not attached:
        con.execute(f"ATTACH '{oracle_db}' AS oracle (READ_ONLY)")

    exists = con.execute(
        """
        SELECT count(*) FROM information_schema.tables
        WHERE table_catalog = 'oracle' AND table_schema = 'mimiciv_derived'
          AND table_name = 'age'
        """
    ).fetchone()[0]
    if not exists:
        return {"ran": False, "reason": "oracle has no mimiciv_derived.age"}

    row = con.execute(
        """
        SELECT count(*) AS n_joined
             , sum(CASE WHEN d.age IS DISTINCT FROM c.age_canonical THEN 1 ELSE 0 END)
               AS n_mismatch
        FROM age_compare c
        JOIN oracle.mimiciv_derived.age d ON d.hadm_id = c.hadm_id
        """
    ).fetchone()
    return {"ran": True, "n_joined": row[0], "n_mismatch": row[1]}


def anchor_year_identities(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Can `anchor_year` be recovered from anything a FHIR consumer can see?

    Each candidate is a year derivable from timestamps MIMIC-on-FHIR actually
    publishes. A candidate that matched `anchor_year` for 100% of patients
    would make the whole defect recoverable downstream; the point of this pass
    is that none does.
    """
    candidates = {
        # Patient.birthDate's own basis, also Encounter.location[].period.start
        "year(MIN(transfers.intime))": "YEAR(min_intime)",
        "year(MAX(transfers.intime))": "YEAR(max_intime)",
        # Encounter.period.start over hospital Encounters
        "year(MIN(admissions.admittime))": "YEAR(min_admittime)",
        "year(MAX(admissions.admittime))": "YEAR(max_admittime)",
        # Encounter.period.end
        "year(MIN(admissions.dischtime))": "YEAR(min_dischtime)",
        "year(MAX(admissions.dischtime))": "YEAR(max_dischtime)",
    }

    # One row per subject, so every candidate below is a plain scalar
    # expression over this view.
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW subject_spans AS
        SELECT
            p.subject_id
            , p.anchor_year
            , t.min_intime, t.max_intime
            , a.min_admittime, a.max_admittime, a.min_dischtime, a.max_dischtime
        FROM patients p
        LEFT JOIN (
            SELECT subject_id, MIN(intime) min_intime, MAX(intime) max_intime
            FROM transfers GROUP BY subject_id
        ) t ON t.subject_id = p.subject_id
        LEFT JOIN (
            SELECT subject_id,
                   MIN(admittime) min_admittime, MAX(admittime) max_admittime,
                   MIN(dischtime) min_dischtime, MAX(dischtime) max_dischtime
            FROM admissions GROUP BY subject_id
        ) a ON a.subject_id = p.subject_id
        """
    )

    results = []
    for label, expr in candidates.items():
        row = con.execute(
            f"""
            SELECT count(*) AS n
                 , sum(CASE WHEN {expr} = anchor_year THEN 1 ELSE 0 END) AS n_exact
            FROM subject_spans
            """
        ).fetchone()
        n, n_exact = row[0], row[1] or 0
        results.append(
            {
                "candidate": label,
                "patients": n,
                "exact": n_exact,
                "pct_exact": round(100.0 * n_exact / n, 4) if n else None,
                "is_identity": n_exact == n,
            }
        )
    return results


def downstream_impact(con: duckdb.DuckDBPyConnection) -> dict:
    """What the age error does to the four official concepts that read `age`."""
    out = {}

    for name, band in (
        ("charlson", CHARLSON_BAND),
        ("sapsii", SAPSII_BAND),
        ("oasis", OASIS_BAND),
    ):
        row = con.execute(
            f"""
            SELECT count(*) AS n_adm
                 , sum(CASE WHEN {band.format(a='age_canonical')}
                            IS DISTINCT FROM {band.format(a='age_yeardiff')}
                       THEN 1 ELSE 0 END) AS n_band_changed
                 , count(DISTINCT CASE WHEN {band.format(a='age_canonical')}
                            IS DISTINCT FROM {band.format(a='age_yeardiff')}
                       THEN subject_id END) AS n_subjects
            FROM age_compare
            """
        ).fetchone()
        out[name] = {
            "admissions": row[0],
            "age_score_changed": row[1],
            "subjects_affected": row[2],
        }

    # creatinine_baseline has no bands -- `age` enters mdrd_est continuously, so
    # every wrong age is a wrong estimate. It also has an `age >= 18` cohort
    # filter, which the error can push a row across in either direction.
    row = con.execute(
        """
        SELECT
            sum(CASE WHEN age_canonical >= 18 THEN 1 ELSE 0 END) AS n_cohort_canonical
          , sum(CASE WHEN age_yeardiff  >= 18 THEN 1 ELSE 0 END) AS n_cohort_published
          , sum(CASE WHEN age_canonical >= 18 AND age_yeardiff < 18 THEN 1 ELSE 0 END)
            AS n_dropped
          , sum(CASE WHEN age_canonical < 18 AND age_yeardiff >= 18 THEN 1 ELSE 0 END)
            AS n_added
          , sum(CASE WHEN age_canonical >= 18 AND age_yeardiff >= 18
                      AND age_canonical <> age_yeardiff THEN 1 ELSE 0 END)
            AS n_mdrd_changed
        FROM age_compare
        """
    ).fetchone()
    out["creatinine_baseline"] = {
        "cohort_canonical": row[0],
        "cohort_published": row[1],
        "rows_dropped_from_cohort": row[2],
        "rows_added_to_cohort": row[3],
        "mdrd_est_changed": row[4],
    }
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mimic-root", help="MIMIC-IV v2.2 distribution root (has hosp/)")
    ap.add_argument("--oracle-db", help="prebuilt DuckDB, for the cross-check")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--memory-limit", default="32GB")
    ap.add_argument("--temp-dir", default=None)
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET threads = {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    if args.temp_dir:
        con.execute(f"SET temp_directory = '{args.temp_dir}'")

    source_desc = build_sources(con, args)
    attached = not bool(args.mimic_root) and bool(args.oracle_db)
    build_core(con)

    cross = check_against_oracle(con, args.oracle_db, attached)
    if cross.get("ran") and cross["n_mismatch"] != 0:
        sys.exit(
            f"ABORT: reproduced canonical age disagrees with mimiciv_derived.age "
            f"on {cross['n_mismatch']}/{cross['n_joined']} rows. The formula "
            f"transcription is wrong; no result below would be trustworthy."
        )

    # ---- headline counts ----------------------------------------------------
    base = con.execute(
        """
        SELECT
            (SELECT count(*) FROM patients)                          AS patients_total
          , (SELECT count(*) FROM published_patient)                 AS patients_published
          , (SELECT count(*) FROM admissions)                        AS admissions_total
          , (SELECT count(*) FROM age_compare)                       AS admissions_scored
        """
    ).df().iloc[0].to_dict()

    tier1 = con.execute(
        """
        SELECT count(*) AS n_adm
             , sum(CASE WHEN age_yeardiff <> age_canonical THEN 1 ELSE 0 END) AS n_wrong
             , count(DISTINCT CASE WHEN age_yeardiff <> age_canonical
                    THEN subject_id END) AS n_subjects
             , min(age_yeardiff - age_canonical) AS min_err
             , max(age_yeardiff - age_canonical) AS max_err
        FROM age_compare
        """
    ).df().iloc[0].to_dict()

    tier2 = con.execute(
        """
        SELECT sum(CASE WHEN age_elapsed <> age_canonical THEN 1 ELSE 0 END) AS n_wrong
             , count(DISTINCT CASE WHEN age_elapsed <> age_canonical
                    THEN subject_id END) AS n_subjects
             , min(age_elapsed - age_canonical) AS min_err
             , max(age_elapsed - age_canonical) AS max_err
        FROM age_compare
        """
    ).df().iloc[0].to_dict()

    fixed = con.execute(
        """
        SELECT sum(CASE WHEN age_yeardiff_fixed <> age_canonical THEN 1 ELSE 0 END)
                 AS n_wrong_yeardiff
             , sum(CASE WHEN age_elapsed_fixed <> age_canonical THEN 1 ELSE 0 END)
                 AS n_wrong_elapsed
        FROM age_compare
        """
    ).df().iloc[0].to_dict()

    # Edge case worth reporting rather than hiding: for a patient whose first
    # event precedes their anchor year, a birth date anchored to
    # `anchor_year - anchor_age` can postdate that event. This is not introduced
    # by the fix -- canonical age.sql already returns a negative age for those
    # rows -- but the maintainers should see the count before adopting it.
    edge = con.execute(
        """
        SELECT
            (SELECT count(*) FROM published_patient
              WHERE birth_date_fixed > CAST(min_intime AS DATE))
              AS patients_fixed_birthdate_after_first_event
          , (SELECT count(*) FROM published_patient
              WHERE birth_date_published > CAST(min_intime AS DATE))
              AS patients_published_birthdate_after_first_event
          , (SELECT count(*) FROM age_compare WHERE age_canonical < 0)
              AS admissions_canonical_age_negative
          , (SELECT count(*) FROM published_patient WHERE anchor_age = 0)
              AS patients_anchor_age_zero
        """
    ).df().iloc[0].to_dict()

    # ---- distributions and worked examples ----------------------------------
    err_dist = con.execute(
        """
        SELECT age_yeardiff - age_canonical AS error_years
             , count(*) AS admissions
             , count(DISTINCT subject_id) AS subjects
        FROM age_compare
        GROUP BY 1 ORDER BY 1
        """
    ).df()

    examples = con.execute(
        """
        SELECT subject_id, hadm_id, admittime, anchor_age, anchor_year, min_intime
             , birth_date_published, birth_date_fixed
             , age_canonical, age_yeardiff, age_yeardiff - age_canonical AS error_years
        FROM age_compare
        WHERE age_yeardiff <> age_canonical
        ORDER BY abs(age_yeardiff - age_canonical) DESC, subject_id, admittime
        LIMIT 50
        """
    ).df()

    affected = con.execute(
        """
        SELECT subject_id, anchor_age, anchor_year, YEAR(min_intime) AS year_min_intime
             , YEAR(min_intime) - anchor_year AS error_years
             , count(*) AS admissions
        FROM age_compare
        WHERE age_yeardiff <> age_canonical
        GROUP BY subject_id, anchor_age, anchor_year, YEAR(min_intime)
        ORDER BY abs(YEAR(min_intime) - anchor_year) DESC, subject_id
        """
    ).df()

    identities = anchor_year_identities(con)
    downstream = downstream_impact(con)

    # ---- write --------------------------------------------------------------
    err_dist.to_csv(out / "error_distribution.csv", index=False)
    examples.to_csv(out / "examples.csv", index=False)
    affected.to_csv(out / "affected_patients.csv", index=False)
    __import__("pandas").DataFrame(identities).to_csv(
        out / "anchor_year_identities.csv", index=False
    )

    n_adm = int(base["admissions_scored"])
    summary = {
        "source": source_desc,
        "cross_check_vs_mimiciv_derived_age": cross,
        "cohort": {k: int(v) for k, v in base.items()},
        "tier1_calendar_year_age": {
            "admissions_wrong": int(tier1["n_wrong"]),
            "subjects_wrong": int(tier1["n_subjects"]),
            "pct_admissions": round(100.0 * int(tier1["n_wrong"]) / n_adm, 4),
            "error_years_min": int(tier1["min_err"]),
            "error_years_max": int(tier1["max_err"]),
        },
        "tier2_elapsed_time_age": {
            "admissions_wrong": int(tier2["n_wrong"]),
            "subjects_wrong": int(tier2["n_subjects"]),
            "pct_admissions": round(100.0 * int(tier2["n_wrong"]) / n_adm, 4),
            "error_years_min": int(tier2["min_err"]),
            "error_years_max": int(tier2["max_err"]),
        },
        "after_proposed_fix": {
            "admissions_wrong_calendar_year": int(fixed["n_wrong_yeardiff"]),
            "admissions_wrong_elapsed_time": int(fixed["n_wrong_elapsed"]),
        },
        "edge_cases": {k: int(v) for k, v in edge.items()},
        "anchor_year_recovery_candidates": identities,
        "downstream_concepts": downstream,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))

    lines: list[str] = []
    w = lines.append
    w("MIMIC-on-FHIR `Patient.birthDate` anchor defect")
    w("=" * 62)
    w(f"source: {source_desc}")
    if cross.get("ran"):
        w(f"cross-check vs mimiciv_derived.age: {cross['n_mismatch']} mismatches "
          f"on {cross['n_joined']} joined rows  <- must be 0")
    else:
        w(f"cross-check vs mimiciv_derived.age: SKIPPED ({cross['reason']})")
    w("")
    w(f"patients in source            : {int(base['patients_total']):,}")
    w(f"patients given a Patient      : {int(base['patients_published']):,}")
    w(f"admissions scored             : {n_adm:,}")
    w("")
    w("-- Tier 1: calendar-year age from the published birthDate ---------------")
    w(f"  admissions with a wrong age : {int(tier1['n_wrong']):,} "
      f"({100.0 * int(tier1['n_wrong']) / n_adm:.4f}%)")
    w(f"  patients affected           : {int(tier1['n_subjects']):,}")
    w(f"  error range (years)         : {int(tier1['min_err'])} .. {int(tier1['max_err'])}")
    w("")
    w("-- Tier 2: elapsed-time age from the published birthDate ----------------")
    w(f"  admissions with a wrong age : {int(tier2['n_wrong']):,} "
      f"({100.0 * int(tier2['n_wrong']) / n_adm:.4f}%)")
    w(f"  patients affected           : {int(tier2['n_subjects']):,}")
    w(f"  error range (years)         : {int(tier2['min_err'])} .. {int(tier2['max_err'])}")
    w("")
    w("-- After the proposed one-line fix --------------------------------------")
    w(f"  calendar-year age wrong     : {int(fixed['n_wrong_yeardiff']):,}")
    w(f"  elapsed-time age wrong      : {int(fixed['n_wrong_elapsed']):,}")
    w("")
    w("-- Edge cases (report, do not hide) -------------------------------------")
    w(f"  patients with anchor_age = 0                    : "
      f"{int(edge['patients_anchor_age_zero']):,}")
    w(f"  admissions whose CANONICAL age is negative      : "
      f"{int(edge['admissions_canonical_age_negative']):,}")
    w(f"  published birthDate after patient's first event : "
      f"{int(edge['patients_published_birthdate_after_first_event']):,}")
    w(f"  fixed birthDate after patient's first event     : "
      f"{int(edge['patients_fixed_birthdate_after_first_event']):,}")
    w("")
    w("-- Can `anchor_year` be recovered from published timestamps? ------------")
    for r in identities:
        flag = "IDENTITY" if r["is_identity"] else "not an identity"
        w(f"  {r['candidate']:<36} {r['exact']:>9,}/{r['patients']:,} "
          f"({r['pct_exact']:.4f}%)  {flag}")
    w("")
    w("-- Error distribution (calendar-year age) -------------------------------")
    for _, r in err_dist.iterrows():
        w(f"  {int(r['error_years']):+3d} years : {int(r['admissions']):>9,} admissions"
          f"  {int(r['subjects']):>7,} patients")
    w("")
    w("-- Downstream official concepts that read mimiciv_derived.age -----------")
    for name in ("charlson", "sapsii", "oasis"):
        d = downstream[name]
        w(f"  {name:<10} age_score changes on {d['age_score_changed']:,} "
          f"admissions ({d['subjects_affected']:,} patients)")
    d = downstream["creatinine_baseline"]
    w(f"  creatinine_baseline  mdrd_est changes on {d['mdrd_est_changed']:,} rows; "
      f"cohort (age>=18) {d['cohort_canonical']:,} -> {d['cohort_published']:,} "
      f"({d['rows_dropped_from_cohort']:,} dropped, {d['rows_added_to_cohort']:,} added)")
    w("")
    w("-- Worst worked examples (see examples.csv) -----------------------------")
    for _, r in examples.head(10).iterrows():
        w(f"  subject {int(r['subject_id'])} hadm {int(r['hadm_id'])} "
          f"admit {r['admittime']} | anchor=({int(r['anchor_age'])},{int(r['anchor_year'])}) "
          f"minT={r['min_intime']} | canonical={int(r['age_canonical'])} "
          f"published={int(r['age_yeardiff'])} ({int(r['error_years']):+d})")

    report = "\n".join(lines) + "\n"
    (out / "report.txt").write_text(report)
    print(report)
    print(f"wrote {out}/report.txt, summary.json and 4 CSVs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
