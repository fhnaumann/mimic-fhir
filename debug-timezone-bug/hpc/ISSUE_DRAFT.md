## Summary

Source timestamps that fall inside the US daylight-saving **spring-forward gap** are silently shifted **+1 hour** during generation, and the original value is unrecoverable in the published FHIR output.

`CAST(<naive timestamp> AS TIMESTAMPTZ)` asks PostgreSQL to interpret a timezone-less MIMIC value in the server's session `TimeZone`. On the second Sunday of March the local hour `02:00`–`02:59` does not exist, so PostgreSQL normalises such a value forward by an hour. A source `admittime` of `02:10` is published as `03:10`.

I found this while mapping concepts off `Encounter.period.start` and tracked it upstream of the mapping into the generation SQL.

## Affected published data

Verified against the **published PhysioNet release [MIMIC-IV on FHIR v2.1](https://physionet.org/content/mimic-iv-fhir/2.1/)** (2024-11-12), file `fhir/MimicEncounter.ndjson.gz`, joined to MIMIC-IV v2.2 `hosp/admissions.csv.gz` on `hadm_id`.

The exact artifact tested, confirmed against the release `SHA256SUMS`:

```
ac2bd7b33edd2303d173913a7b505debe40111a7c09d04d0a139b98cefb7a6e9  fhir/MimicEncounter.ndjson.gz
```

```
encounters compared      : 431,231
identical wall clock     : 431,187
FHIR exactly +1 hour     : 44
other differences        : 0

admissions in the DST gap: 44
  ...of which shifted    : 44 (100%)
```

Every one of the 44 differing rows fits the DST-gap signature exactly, with no exceptions:

| check | count |
|---|---|
| total differing | 44 |
| source hour is `02` | 44 |
| falls on a Sunday | 44 |
| second Sunday of March | 44 |
| FHIR offset is `-04:00` | 44 |

The key number is the last pair: **44 admissions in MIMIC-IV fall inside the gap, and all 44 were shifted.** This is not a sampling artefact — it is a deterministic transformation of every value that enters the gap.

> The 100-patient demo dataset contains **no** admission inside the gap, so this is invisible to demo-based testing. That is likely why it has gone unnoticed.

## Minimal reproduction (no MIMIC data required)

```sql
SET TimeZone = 'America/New_York';   -- the zone the v2.1 release was generated under

SELECT jsonb_build_object(
    'start', CAST('2159-03-11 02:10:00'::timestamp AS TIMESTAMPTZ)
)->>'start';

--        ?column?
-- ---------------------------
--  2159-03-11T03:10:00-04:00      <-- source said 02:10
```

Stepping through the transition window shows why the loss is permanent:

| source `admittime` | published `period.start` | |
|---|---|---|
| `2159-03-11 01:30` | `2159-03-11T01:30:00-05:00` | ok |
| `2159-03-11 02:00` | `2159-03-11T03:00:00-04:00` | **shifted** |
| `2159-03-11 02:30` | `2159-03-11T03:30:00-04:00` | **shifted** |
| `2159-03-11 03:00` | `2159-03-11T03:00:00-04:00` | genuine |
| `2159-03-11 03:30` | `2159-03-11T03:30:00-04:00` | genuine |

Source `02:30` and source `03:30` produce **the identical output string**. No downstream consumer can separate them, so this cannot be repaired after generation.

## Full reproduction against the published release

Requires only `duckdb` (`pip install duckdb`). The comparison is deliberately **wall-clock only** — it takes the first 19 characters of the FHIR string and never parses the offset as an instant, because doing so introduces a separate and much larger local-timezone artefact that masks the one-hour effect.

```
python verify_dst_gap.py \
    --admissions       /path/to/mimic-iv-2.2/hosp/admissions.csv.gz \
    --encounter-ndjson /path/to/fhir/MimicEncounter.ndjson.gz
```

<details>
<summary><code>verify_dst_gap.py</code></summary>

```python
#!/usr/bin/env python3
"""Reproduce the DST spring-forward gap defect in MIMIC-on-FHIR.

Compares mimiciv_hosp.admissions.admittime in the original MIMIC-IV
distribution against Encounter.period.start in the published MIMIC-on-FHIR
ndjson release.

The comparison is wall-clock only: it takes the first 19 characters of the
FHIR dateTime string and never parses the offset as an instant. Parsing the
offset would introduce a separate, larger local-timezone artefact and mask
the one-hour effect being measured.

Requires only duckdb:

    pip install duckdb
    python verify_dst_gap.py \
        --admissions /path/to/mimic-iv-2.2/hosp/admissions.csv.gz \
        --encounter-ndjson /path/to/fhir/MimicEncounter.ndjson.gz
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

HOSP_SYSTEM = 'http://mimic.mit.edu/fhir/mimic/identifier/encounter-hosp'


def lit(value) -> str:
    """Quote a value as a SQL string literal (CREATE VIEW cannot bind params)."""
    return "'" + str(value).replace("'", "''") + "'"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--admissions', type=Path, required=True,
                        help='mimic-iv-2.2/hosp/admissions.csv.gz')
    parser.add_argument('--encounter-ndjson', type=Path, required=True,
                        help='MimicEncounter.ndjson.gz from the FHIR release')
    parser.add_argument('--threads', type=int, default=4)
    args = parser.parse_args()

    for path in (args.admissions, args.encounter_ndjson):
        if not path.is_file():
            print(f'ERROR: not found: {path}', file=sys.stderr)
            return 2

    con = duckdb.connect()
    con.execute(f'SET threads = {args.threads}')
    # Nothing here uses TIMESTAMPTZ. Set explicitly so that the result cannot
    # depend on the machine running this script.
    con.execute("SET TimeZone = 'UTC'")

    con.execute(f"""
        CREATE VIEW admissions AS
        SELECT subject_id, hadm_id, admittime
        FROM read_csv({lit(args.admissions)}, header = true, types = {{
            'subject_id': 'BIGINT', 'hadm_id': 'BIGINT',
            'admittime': 'TIMESTAMP'
        }})
    """)

    # Only two fields are needed, so the schema is given explicitly rather
    # than inferred over the whole file.
    con.execute(f"""
        CREATE VIEW fhir_encounters AS
        WITH raw AS (
            SELECT unnest(identifier) AS ident, period.start AS period_start
            FROM read_json({lit(args.encounter_ndjson)},
                           format = 'newline_delimited',
                           compression = 'gzip', columns = {{
                'identifier': 'STRUCT(system VARCHAR, value VARCHAR)[]',
                'period': 'STRUCT(start VARCHAR, "end" VARCHAR)'
            }})
        )
        SELECT DISTINCT
            TRY_CAST(ident.value AS BIGINT) AS hadm_id,
            period_start,
            -- Keep the wall-clock digits; do not interpret the offset.
            TRY_CAST(substr(period_start, 1, 19) AS TIMESTAMP) AS period_walltime
        FROM raw
        WHERE ident.system = {lit(HOSP_SYSTEM)}
          AND TRY_CAST(ident.value AS BIGINT) IS NOT NULL
    """)

    con.execute("""
        CREATE VIEW comparison AS
        SELECT a.hadm_id, a.subject_id, a.admittime, f.period_start,
               epoch(f.period_walltime - a.admittime) / 3600.0 AS delta_hours
        FROM admissions a
        JOIN fhir_encounters f USING (hadm_id)
    """)

    compared, identical, plus_one, other = con.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE delta_hours = 0),
               count(*) FILTER (WHERE delta_hours = 1),
               count(*) FILTER (WHERE delta_hours NOT IN (0, 1))
        FROM comparison
    """).fetchone()

    # Every MIMIC admission whose wall-clock time falls in the spring-forward
    # gap: 02:00-02:59 on the second Sunday of March (US rule since 2007).
    at_risk = con.execute("""
        SELECT count(*) FROM admissions
        WHERE month(admittime) = 3 AND dayofweek(admittime) = 0
          AND day(admittime) BETWEEN 8 AND 14 AND hour(admittime) = 2
    """).fetchone()[0]

    print(f'encounters compared      : {compared:,}')
    print(f'identical wall clock     : {identical:,}')
    print(f'FHIR exactly +1 hour     : {plus_one:,}')
    print(f'other differences        : {other:,}')
    print()
    print(f'admissions in the DST gap: {at_risk:,}')
    print(f'  ...of which shifted    : {plus_one:,}'
          f' ({"100%" if at_risk and plus_one == at_risk else "see below"})')

    profile = con.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE hour(admittime) = 2),
               count(*) FILTER (WHERE dayname(admittime) = 'Sunday'),
               count(*) FILTER (WHERE month(admittime) = 3
                                  AND day(admittime) BETWEEN 8 AND 14),
               count(*) FILTER (WHERE right(period_start, 6) = '-04:00')
        FROM comparison WHERE delta_hours != 0
    """).fetchone()
    total, at_two, sundays, second_sunday, edt = profile
    print()
    print('Profile of the differing rows (all should equal the total):')
    print(f'  total differing        : {total:,}')
    print(f'  source hour is 02      : {at_two:,}')
    print(f'  falls on a Sunday      : {sundays:,}')
    print(f'  2nd Sunday of March    : {second_sunday:,}')
    print(f'  FHIR offset is -04:00  : {edt:,}')

    rows = con.execute("""
        SELECT hadm_id, subject_id, admittime, period_start, delta_hours
        FROM comparison WHERE delta_hours != 0
        ORDER BY hadm_id LIMIT 10
    """).fetchall()
    if rows:
        print()
        print('First 10 differing rows:')
        print(f'  {"hadm_id":>10}  {"subject_id":>10}  {"MIMIC admittime":>19}'
              f'  {"FHIR period.start":>26}  {"delta":>5}')
        for hadm_id, subject_id, admittime, start, delta in rows:
            print(f'  {hadm_id:>10}  {subject_id:>10}  {str(admittime):>19}'
                  f'  {start:>26}  {delta:>5}')
    return 0


if __name__ == '__main__':
    sys.exit(main())

```

</details>

<details>
<summary>All 44 affected admissions</summary>

The per-admission list (hadm_id, subject_id, admittime) is withheld: it comes
from the full, credentialed MIMIC-IV dataset. `hpc/timezone_gap_debug.py`
reproduces it against a local copy.

</details>

## Root cause

[`sql/fhir_encounter.sql#L65-L66`](https://github.com/kind-lab/mimic-fhir/blob/main/sql/fhir_encounter.sql#L65-L66):

```sql
, CAST(adm.admittime AS TIMESTAMPTZ) AS adm_ADMITTIME
, CAST(adm.dischtime AS TIMESTAMPTZ) AS adm_DISCHTIME
```

These feed `period.start` / `period.end` at [`#L150-L151`](https://github.com/kind-lab/mimic-fhir/blob/main/sql/fhir_encounter.sql#L150-L151) via `jsonb_build_object`, which renders a `timestamptz` as ISO-8601 in the session `TimeZone`.

MIMIC timestamps are timezone-less wall-clock values. Casting them to `TIMESTAMPTZ` forces an interpretation that the source data does not support, and the gap is where that interpretation destroys information.

Because the behaviour depends on the server's `TimeZone`, the corruption is **environment-dependent**: generating under `UTC` produces no shift, while generating under `America/New_York` — as the published v2.1 release evidently was, given its `-04:00`/`-05:00` offsets — produces these 44 defects.

## Scope

I measured only `Encounter.period.start`. The same cast pattern occurs **44 times across 22 SQL files** (`fhir_encounter_icu.sql`, `fhir_medication_request.sql`, `fhir_observation_*.sql`, `fhir_procedure*.sql`, `fhir_specimen*.sql`, …), so every mapped timestamp is subject to the identical failure. `dischtime` on the very next line is almost certainly affected in the same way.

## Suggested fix

The smallest comprehensive change is to generate under UTC:

```sql
SET TimeZone = 'UTC';
```

UTC has no DST, so both the gap and the fall-back ambiguity below disappear, and all 44 cast sites are corrected without editing any of them. Wall-clock digits then match the source exactly for every row.

Note that simply dropping the offset is **not** available: FHIR's `dateTime` requires a timezone once a time is present, so some offset must be attached, and UTC is the only choice with neither a gap nor an ambiguity.

The trade-off worth flagging: this changes the instant each string denotes (e.g. `07:15:00-04:00` = `11:15Z` becomes `07:15:00+00:00` = `07:15Z`), so every timestamp in the release moves 4–5 hours as an instant even though its displayed digits are unchanged. Given that MIMIC timestamps are date-shifted surrogate values with no true instant, the existing `-04:00`/`-05:00` offsets are already arbitrary — but it is a release-wide semantic change and worth a deliberate decision.

## Secondary observation: the fall-back hour

The same cast has a milder counterpart in November. On the first Sunday, local `01:00`–`01:59` occurs twice, and PostgreSQL resolves the ambiguity to the second occurrence (`-05:00`). **53** MIMIC admissions land in that hour and all 53 are published with `-05:00`.

This one is lossless — the wall-clock digits survive, so a wall-clock consumer is unaffected — but roughly half of those rows name an instant an hour away from the intended one. The UTC fix removes this case too.

## Environment

- MIMIC-IV v2.2
- MIMIC-IV on FHIR **v2.1** (PhysioNet, 2024-11-12) — all 30 `fhir/*.ndjson.gz` files verified byte-identical to the published `SHA256SUMS`
- Root cause verified independently in PostgreSQL 18
- Cross-checked against a separate, independently generated copy of the warehouse, which produced **the identical 44 rows**
