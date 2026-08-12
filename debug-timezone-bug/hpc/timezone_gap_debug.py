#!/usr/bin/env python3
"""MIMIC to MIMIC-on-FHIR DST-gap investigation, full-cohort batch version.

This is the batch conversion of ``timezone_gap_debug.ipynb``. It compares the
original ``mimiciv_hosp.admissions.admittime`` with the corresponding hospital
Encounter ``period.start`` in the generated MIMIC-on-FHIR Delta warehouse.

The comparison deliberately preserves the wall-clock digits from the FHIR
string. It does **not** parse the FHIR offset as a real instant, because that
would introduce the separate 13-16 hour local-session-timezone bug.

The goal is to expose the smaller upstream DST-gap transformation: a source
value such as ``02:10`` can be served by FHIR as ``03:10`` when the upstream
ETL casts the naive source timestamp through ``TIMESTAMPTZ``.

Two properties make DuckDB the correct engine here rather than Spark:

* ``period.start`` is stored as a ``VARCHAR`` in the Delta table, so no engine
  can reinterpret it. The reader is immaterial to the value under test.
* DuckDB's ``TIMESTAMP`` is timezone-naive. Spark's ``TimestampType`` is
  instant-based and applies ``spark.sql.session.timeZone``, which is exactly
  the contamination this investigation must avoid.

The single thing Spark would provide natively is Delta snapshot resolution.
That is hand-rolled here, so ``assert_readable_protocol`` refuses to run
against any table whose protocol makes a raw Parquet read unsound.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

import duckdb

# The hand-rolled snapshot resolver below understands plain add/remove files
# only. Reader version 2 introduces column mapping and version 3 introduces
# deletion vectors; under either, the active Parquet files no longer describe
# the table on their own and a raw read would be silently wrong.
MAX_SUPPORTED_READER_VERSION = 1

HOSPITAL_IDENTIFIER_SYSTEM = (
    'http://mimic.mit.edu/fhir/mimic/identifier/encounter-hosp'
)

# Delta commit and checkpoint file names are zero-padded to 20 digits.
COMMIT_RE = re.compile(r'^(\d{20})\.json$')
CHECKPOINT_RE = re.compile(r'^(\d{20})\.checkpoint[.\w-]*\.parquet$')


class SnapshotError(RuntimeError):
    """The Delta table cannot be read safely by this script."""


# --------------------------------------------------------------------------
# Delta snapshot resolution
# --------------------------------------------------------------------------

@dataclass
class Snapshot:
    """The resolved active file set of a Delta table."""

    table_dir: Path
    version: int
    files: list[Path]
    protocol: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    checkpoint_version: int | None = None
    commits_applied: list[int] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def sql_literal(value: str) -> str:
    """Quote a value as a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def sql_list(values) -> str:
    """Render a Python iterable as a SQL list literal."""
    return '[' + ', '.join(sql_literal(v) for v in values) + ']'


def _checkpoint_files(log_dir: Path, version: int) -> list[Path]:
    """All parts of the checkpoint at ``version``, sorted."""
    prefix = f'{version:020d}.checkpoint'
    return sorted(
        path for path in log_dir.iterdir()
        if path.name.startswith(prefix) and path.name.endswith('.parquet')
    )


def _read_checkpoint(con, files: list[Path]) -> tuple[set[str], dict, dict]:
    """Return (active paths, protocol, metadata) recorded in a checkpoint.

    A checkpoint stores the already-reconciled state, so every ``add`` row in
    it is an active file. ``remove`` rows are retained tombstones and are
    deliberately ignored.

    ``to_json`` is used for the protocol and metadata structs so that this
    function does not depend on which optional nested fields the writer chose
    to materialise in the checkpoint schema.
    """
    listed = sql_list(str(f) for f in files)

    adds = con.execute(
        f'SELECT add.path FROM read_parquet({listed}) '
        'WHERE add IS NOT NULL AND add.path IS NOT NULL'
    ).fetchall()

    protocol_rows = con.execute(
        f'SELECT to_json(protocol) FROM read_parquet({listed}) '
        'WHERE protocol IS NOT NULL'
    ).fetchall()
    metadata_rows = con.execute(
        f'SELECT to_json(metaData) FROM read_parquet({listed}) '
        'WHERE metaData IS NOT NULL'
    ).fetchall()

    protocol = json.loads(protocol_rows[-1][0]) if protocol_rows else {}
    metadata = json.loads(metadata_rows[-1][0]) if metadata_rows else {}
    return {unquote(row[0]) for row in adds}, protocol, metadata


def _read_commit(path: Path) -> list[dict]:
    """Parse one newline-delimited JSON Delta commit file."""
    actions = []
    with path.open('r', encoding='utf-8') as handle:
        for line in handle:
            line = line.strip()
            if line:
                actions.append(json.loads(line))
    return actions


def resolve_snapshot(con, table_dir: Path) -> Snapshot:
    """Resolve the current active Parquet file set of a Delta table.

    The state is seeded from the newest checkpoint and then advanced by every
    JSON commit after it. Within a commit, removes are applied before adds so
    that a path both removed and re-added in the same commit stays active.
    """
    log_dir = table_dir / '_delta_log'
    if not log_dir.is_dir():
        raise SnapshotError(f'Not a Delta table, no _delta_log: {table_dir}')

    notes: list[str] = []

    # Coordinated-commit tables can hold commits here that are not yet
    # backfilled into the main log. Applying them requires catalog ordering
    # information this script does not have.
    staged_dir = log_dir / '_staged_commits'
    if staged_dir.is_dir():
        staged = sorted(p.name for p in staged_dir.glob('*.json'))
        if staged:
            raise SnapshotError(
                f'{len(staged)} un-backfilled staged commit(s) in '
                f'{staged_dir}. This table uses coordinated commits and the '
                f'resolved snapshot would be stale. First staged: {staged[0]}'
            )
        notes.append('_staged_commits present but empty (no pending commits)')

    commits: dict[int, Path] = {}
    checkpoint_versions: set[int] = set()
    for path in log_dir.iterdir():
        commit_match = COMMIT_RE.match(path.name)
        if commit_match:
            commits[int(commit_match.group(1))] = path
            continue
        checkpoint_match = CHECKPOINT_RE.match(path.name)
        if checkpoint_match:
            checkpoint_versions.add(int(checkpoint_match.group(1)))

    if not commits and not checkpoint_versions:
        raise SnapshotError(f'Empty Delta log: {log_dir}')

    # _last_checkpoint is a hint. Trust it only if the files are really there.
    checkpoint_version: int | None = None
    expected_add_files: int | None = None
    hint_path = log_dir / '_last_checkpoint'
    if hint_path.is_file():
        try:
            hint = json.loads(hint_path.read_text(encoding='utf-8'))
            hinted = int(hint['version'])
            expected_add_files = hint.get('numOfAddFiles')
            if hinted in checkpoint_versions:
                checkpoint_version = hinted
            else:
                notes.append(
                    f'_last_checkpoint names version {hinted} but no such '
                    f'checkpoint file exists; falling back to log scan'
                )
        except (ValueError, KeyError) as error:
            notes.append(f'_last_checkpoint unreadable ({error}); scanning log')

    if checkpoint_version is None and checkpoint_versions:
        checkpoint_version = max(checkpoint_versions)

    active: set[str] = set()
    protocol: dict = {}
    metadata: dict = {}

    if checkpoint_version is not None:
        parts = _checkpoint_files(log_dir, checkpoint_version)
        active, protocol, metadata = _read_checkpoint(con, parts)
        notes.append(
            f'seeded from checkpoint v{checkpoint_version} '
            f'({len(parts)} part(s), {len(active)} add file(s))'
        )
        if expected_add_files is not None and expected_add_files != len(active):
            notes.append(
                f'WARNING: _last_checkpoint declares numOfAddFiles='
                f'{expected_add_files} but {len(active)} were read'
            )

    floor = -1 if checkpoint_version is None else checkpoint_version
    applied: list[int] = []
    for version in sorted(v for v in commits if v > floor):
        adds, removes = set(), set()
        for action in _read_commit(commits[version]):
            if 'add' in action:
                adds.add(unquote(action['add']['path']))
            elif 'remove' in action:
                removes.add(unquote(action['remove']['path']))
            elif 'protocol' in action:
                protocol = action['protocol']
            elif 'metaData' in action:
                metadata = action['metaData']
        active -= removes
        active |= adds
        applied.append(version)

    # If the log was truncated past every protocol/metadata declaration, fall
    # back to scanning the surviving commits oldest-first.
    if not protocol or not metadata:
        for version in sorted(commits):
            for action in _read_commit(commits[version]):
                if 'protocol' in action and not protocol:
                    protocol = action['protocol']
                if 'metaData' in action and not metadata:
                    metadata = action['metaData']

    latest = max(
        [checkpoint_version if checkpoint_version is not None else -1]
        + applied
        + ([max(commits)] if commits else [])
    )

    files = [table_dir / relative for relative in sorted(active)]
    missing = [f for f in files if not f.is_file()]
    if missing:
        raise SnapshotError(
            f'{len(missing)} active file(s) named in the Delta log are absent '
            f'from disk, first: {missing[0]}'
        )
    if not files:
        raise SnapshotError(f'Snapshot of {table_dir} contains no data files')

    return Snapshot(
        table_dir=table_dir,
        version=latest,
        files=files,
        protocol=protocol,
        metadata=metadata,
        checkpoint_version=checkpoint_version,
        commits_applied=applied,
        notes=notes,
    )


def assert_readable_protocol(snapshot: Snapshot) -> None:
    """Refuse to continue if a raw Parquet read of the snapshot is unsound.

    This is the guard that replaces Spark. Rather than reimplementing advanced
    Delta reader features, the script fails loudly the moment one appears.
    """
    protocol = snapshot.protocol
    if not protocol:
        raise SnapshotError(
            'No protocol action found in the Delta log; cannot confirm that a '
            'raw Parquet read is sound.'
        )

    reader_version = protocol.get('minReaderVersion')
    if reader_version is None:
        raise SnapshotError(f'Protocol action has no minReaderVersion: {protocol}')
    if reader_version > MAX_SUPPORTED_READER_VERSION:
        raise SnapshotError(
            f'Table requires Delta reader version {reader_version}, this '
            f'script supports up to {MAX_SUPPORTED_READER_VERSION}. Reader '
            f'v2 adds column mapping and v3 adds deletion vectors; under '
            f'either, reading the active Parquet files directly would give '
            f'wrong answers. Protocol: {protocol}'
        )

    reader_features = protocol.get('readerFeatures') or []
    if reader_features:
        raise SnapshotError(
            f'Table declares reader features {sorted(reader_features)}, which '
            f'this script does not implement.'
        )

    partition_columns = snapshot.metadata.get('partitionColumns') or []
    if partition_columns:
        raise SnapshotError(
            f'Table is partitioned by {partition_columns}. Partition values '
            f'live in the file paths, not the Parquet data, so a raw read '
            f'would drop them.'
        )


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------

def build_views(con, admissions_csv: Path, snapshot: Snapshot) -> None:
    """Create the oracle, FHIR and comparison views."""
    # Types are pinned rather than sniffed. admittime must land in DuckDB's
    # timezone-naive TIMESTAMP so that the source wall-clock digits survive.
    con.execute(f"""
        CREATE OR REPLACE VIEW oracle_admissions AS
        SELECT
            subject_id,
            hadm_id,
            admittime AS oracle_admittime
        FROM read_csv(
            {sql_literal(admissions_csv)},
            header = true,
            types = {{
                'subject_id': 'BIGINT',
                'hadm_id': 'BIGINT',
                'admittime': 'TIMESTAMP'
            }}
        )
    """)

    listed = sql_list(str(f) for f in snapshot.files)
    con.execute(f"""
        CREATE OR REPLACE VIEW fhir_hospital_encounters AS
        WITH flattened AS (
            SELECT
                unnest(identifier) AS ident,
                period.start AS fhir_period_start
            FROM read_parquet({listed}, union_by_name = true)
        ), hospital_encounters AS (
            SELECT DISTINCT
                TRY_CAST(ident.value AS BIGINT) AS hadm_id,
                fhir_period_start
            FROM flattened
            WHERE ident.system = {sql_literal(HOSPITAL_IDENTIFIER_SYSTEM)}
        )
        SELECT
            hadm_id,
            fhir_period_start,
            -- Keep the FHIR wall-clock digits. Do not cast the offset-bearing
            -- string to TIMESTAMPTZ during this diagnostic comparison.
            TRY_CAST(substr(fhir_period_start, 1, 19) AS TIMESTAMP)
                AS fhir_period_walltime
        FROM hospital_encounters
        WHERE hadm_id IS NOT NULL
    """)

    # FULL OUTER so that coverage gaps on either side are measurable rather
    # than silently dropped, which an INNER JOIN would do on full data.
    con.execute("""
        CREATE OR REPLACE VIEW timezone_comparison AS
        SELECT
            coalesce(a.hadm_id, f.hadm_id) AS hadm_id,
            a.subject_id,
            a.oracle_admittime,
            f.fhir_period_start,
            f.fhir_period_walltime,
            epoch(f.fhir_period_walltime - a.oracle_admittime) / 3600.0
                AS delta_hours
        FROM oracle_admissions AS a
        FULL OUTER JOIN fhir_hospital_encounters AS f
            ON a.hadm_id = f.hadm_id
    """)


def one(con, sql: str):
    """Run a scalar query."""
    return con.execute(sql).fetchone()[0]


def collect_results(con, out_dir: Path, sample_limit: int) -> dict:
    """Run every summary query and write the CSV artefacts."""
    results: dict = {}

    results['oracle_admission_rows'] = one(
        con, 'SELECT count(*) FROM oracle_admissions')
    results['oracle_distinct_hadm_ids'] = one(
        con, 'SELECT count(DISTINCT hadm_id) FROM oracle_admissions')
    results['fhir_hospital_encounter_rows'] = one(
        con, 'SELECT count(*) FROM fhir_hospital_encounters')
    results['fhir_distinct_hadm_ids'] = one(
        con, 'SELECT count(DISTINCT hadm_id) FROM fhir_hospital_encounters')

    # A hadm_id carrying two different period.start values would fan the join
    # out and make every count below misleading.
    results['fhir_hadm_ids_with_conflicting_starts'] = one(con, """
        SELECT count(*) FROM (
            SELECT hadm_id
            FROM fhir_hospital_encounters
            GROUP BY hadm_id
            HAVING count(DISTINCT fhir_period_start) > 1
        )
    """)

    results['fhir_unparseable_period_start'] = one(con, """
        SELECT count(*) FROM fhir_hospital_encounters
        WHERE fhir_period_start IS NOT NULL AND fhir_period_walltime IS NULL
    """)

    matched = 'oracle_admittime IS NOT NULL AND fhir_period_start IS NOT NULL'
    results['matched_rows'] = one(
        con, f'SELECT count(*) FROM timezone_comparison WHERE {matched}')
    results['oracle_only_rows'] = one(con, """
        SELECT count(*) FROM timezone_comparison
        WHERE fhir_period_start IS NULL
    """)
    results['fhir_only_rows'] = one(con, """
        SELECT count(*) FROM timezone_comparison
        WHERE oracle_admittime IS NULL
    """)

    summary = con.execute(f"""
        SELECT
            count(*) FILTER (WHERE delta_hours = 0) AS exact_wall_clock_rows,
            count(*) FILTER (WHERE delta_hours = 1) AS plus_one_hour_rows,
            count(*) FILTER (WHERE delta_hours = -1) AS minus_one_hour_rows,
            count(*) FILTER (WHERE delta_hours != 0) AS any_difference_rows,
            min(delta_hours) AS minimum_delta_hours,
            max(delta_hours) AS maximum_delta_hours
        FROM timezone_comparison
        WHERE {matched}
    """).fetchone()
    results.update(dict(zip([
        'exact_wall_clock_rows',
        'plus_one_hour_rows',
        'minus_one_hour_rows',
        'any_difference_rows',
        'minimum_delta_hours',
        'maximum_delta_hours',
    ], summary)))

    def dump(name: str, sql: str) -> Path:
        path = out_dir / name
        con.execute(
            f"COPY ({sql}) TO {sql_literal(path)} (FORMAT CSV, HEADER)")
        return path

    # Full distribution of wall-clock differences.
    dump('delta_hours_distribution.csv', f"""
        SELECT delta_hours, count(*) AS row_count
        FROM timezone_comparison
        WHERE {matched}
        GROUP BY delta_hours
        ORDER BY abs(delta_hours) DESC, delta_hours
    """)
    results['delta_hours_distribution'] = con.execute(f"""
        SELECT delta_hours, count(*) AS row_count
        FROM timezone_comparison
        WHERE {matched}
        GROUP BY delta_hours
        ORDER BY abs(delta_hours) DESC, delta_hours
    """).fetchall()

    divergent = f'{matched} AND delta_hours != 0'

    dump('divergences.csv', f"""
        SELECT hadm_id, subject_id, oracle_admittime, fhir_period_start,
               fhir_period_walltime, delta_hours
        FROM timezone_comparison
        WHERE {divergent}
        ORDER BY abs(delta_hours) DESC, hadm_id
    """)

    # A DST spring-forward gap predicts that divergent source values cluster
    # in the 02:00-02:59 local hour.
    dump('divergent_source_hour_histogram.csv', f"""
        SELECT
            hour(oracle_admittime) AS oracle_hour,
            delta_hours,
            count(*) AS row_count
        FROM timezone_comparison
        WHERE {divergent}
        GROUP BY oracle_hour, delta_hours
        ORDER BY row_count DESC, oracle_hour
    """)
    results['divergent_source_hour_histogram'] = con.execute(f"""
        SELECT hour(oracle_admittime) AS oracle_hour, count(*) AS row_count
        FROM timezone_comparison
        WHERE {divergent}
        GROUP BY oracle_hour
        ORDER BY row_count DESC, oracle_hour
    """).fetchall()

    # A spring-forward transition also predicts a single calendar date per
    # year, always a Sunday.
    dump('divergent_source_dates.csv', f"""
        SELECT
            CAST(oracle_admittime AS DATE) AS oracle_date,
            dayname(oracle_admittime) AS oracle_day_name,
            count(*) AS row_count
        FROM timezone_comparison
        WHERE {divergent}
        GROUP BY oracle_date, oracle_day_name
        ORDER BY oracle_date
    """)
    results['divergent_day_names'] = con.execute(f"""
        SELECT dayname(oracle_admittime) AS day_name, count(*) AS row_count
        FROM timezone_comparison
        WHERE {divergent}
        GROUP BY day_name
        ORDER BY row_count DESC
    """).fetchall()

    if results['oracle_only_rows']:
        dump('unmatched_oracle_admissions.csv', """
            SELECT hadm_id, subject_id, oracle_admittime
            FROM timezone_comparison
            WHERE fhir_period_start IS NULL
            ORDER BY hadm_id
        """)
    if results['fhir_only_rows']:
        dump('unmatched_fhir_encounters.csv', """
            SELECT hadm_id, fhir_period_start, fhir_period_walltime
            FROM timezone_comparison
            WHERE oracle_admittime IS NULL
            ORDER BY hadm_id
        """)

    results['divergent_sample'] = con.execute(f"""
        SELECT hadm_id, subject_id, oracle_admittime, fhir_period_start,
               delta_hours
        FROM timezone_comparison
        WHERE {divergent}
        ORDER BY abs(delta_hours) DESC, hadm_id
        LIMIT {int(sample_limit)}
    """).fetchall()

    return results


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def render_report(results: dict, snapshot: Snapshot, config: dict) -> str:
    """Build the human-readable report."""
    lines: list[str] = []
    add = lines.append

    def rule(title: str) -> None:
        add('')
        add(f'== {title} ' + '=' * max(0, 74 - len(title)))

    add('MIMIC to MIMIC-on-FHIR DST-gap investigation')
    add(f'Generated: {datetime.now(timezone.utc).isoformat(timespec="seconds")}')

    rule('Inputs')
    for key, value in config.items():
        add(f'  {key}: {value}')

    rule('Delta snapshot')
    add(f'  table: {snapshot.table_dir}')
    add(f'  resolved version: {snapshot.version}')
    add(f'  checkpoint version: {snapshot.checkpoint_version}')
    add(f'  commits applied after checkpoint: {snapshot.commits_applied or "none"}')
    add(f'  active data files: {len(snapshot.files)}')
    add(f'  protocol: {json.dumps(snapshot.protocol)}')
    for note in snapshot.notes:
        add(f'  note: {note}')

    rule('Row counts')
    add(f'  original admissions rows: {results["oracle_admission_rows"]:,}')
    add(f'  original distinct hadm_id: {results["oracle_distinct_hadm_ids"]:,}')
    add(f'  FHIR hospital Encounter rows: {results["fhir_hospital_encounter_rows"]:,}')
    add(f'  FHIR distinct hadm_id: {results["fhir_distinct_hadm_ids"]:,}')
    add(f'  matched on hadm_id: {results["matched_rows"]:,}')
    add(f'  in MIMIC but not in FHIR: {results["oracle_only_rows"]:,}')
    add(f'  in FHIR but not in MIMIC: {results["fhir_only_rows"]:,}')

    conflicting = results['fhir_hadm_ids_with_conflicting_starts']
    if conflicting:
        add('')
        add(f'  WARNING: {conflicting:,} hadm_id(s) carry more than one '
            f'distinct period.start. The join fans out on these and the')
        add('           counts below are inflated for them.')
    unparseable = results['fhir_unparseable_period_start']
    if unparseable:
        add(f'  WARNING: {unparseable:,} period.start value(s) could not be '
            f'parsed as a wall-clock timestamp.')

    rule('Wall-clock comparison')
    add(f'  identical wall clock: {results["exact_wall_clock_rows"]:,}')
    add(f'  FHIR exactly +1 hour: {results["plus_one_hour_rows"]:,}')
    add(f'  FHIR exactly -1 hour: {results["minus_one_hour_rows"]:,}')
    add(f'  any difference: {results["any_difference_rows"]:,}')
    add(f'  minimum delta_hours: {results["minimum_delta_hours"]}')
    add(f'  maximum delta_hours: {results["maximum_delta_hours"]}')

    if results['delta_hours_distribution']:
        rule('delta_hours distribution')
        add(f'  {"delta_hours":>14}  {"rows":>14}')
        for delta, count in results['delta_hours_distribution'][:40]:
            add(f'  {delta:>14}  {count:>14,}')

    if results['divergent_source_hour_histogram']:
        rule('Source hour of divergent rows')
        add('  A spring-forward gap predicts a cluster at hour 02.')
        add(f'  {"hour":>6}  {"rows":>14}')
        for hour, count in results['divergent_source_hour_histogram'][:24]:
            marker = '  <- DST gap hour' if hour == 2 else ''
            add(f'  {hour:>6}  {count:>14,}{marker}')

    if results['divergent_day_names']:
        rule('Weekday of divergent rows')
        add('  US DST transitions always fall on a Sunday.')
        for day_name, count in results['divergent_day_names']:
            add(f'  {day_name:>10}  {count:>14,}')

    if results['divergent_sample']:
        rule('Sample divergent rows')
        add(f'  {"hadm_id":>10}  {"subject_id":>10}  {"MIMIC admittime":>19}  '
            f'{"FHIR period.start":>26}  {"delta":>6}')
        for hadm_id, subject_id, admittime, start, delta in results['divergent_sample']:
            add(f'  {hadm_id:>10}  {subject_id:>10}  {str(admittime):>19}  '
                f'{str(start):>26}  {delta:>6}')

    rule('Verdict')
    plus_one = results['plus_one_hour_rows']
    any_diff = results['any_difference_rows']
    if plus_one:
        add(f'  {plus_one:,} row(s) have a FHIR wall-clock time exactly one hour')
        add('  later than the original. This is the expected DST-gap shape: an')
        add('  original 02:xx value was served as 03:xx. The one-hour difference')
        add('  was introduced before the concept mapping ran.')
    elif any_diff:
        add(f'  No +1 hour rows, but {any_diff:,} row(s) differ by some other')
        add('  amount. See delta_hours_distribution.csv; this is not the')
        add('  DST-gap shape and needs separate explanation.')
    else:
        add('  No wall-clock differences at all across the full cohort. The')
        add('  comparison ran end to end, so the DST-gap transformation is not')
        add('  present in this snapshot of the warehouse.')

    add('')
    return '\n'.join(lines)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--mimic-root', type=Path,
        default=Path(os.environ.get(
            'MIMIC_ROOT', os.path.expandvars('$SCRATCH/original-mimic-iv/mimic-iv-2.2'))),
        help='Root of the original MIMIC-IV distribution (contains hosp/).')
    parser.add_argument(
        '--fhir-warehouse', type=Path,
        default=Path(os.environ.get(
            'MIMIC_FHIR_WAREHOUSE',
            os.path.expandvars('$SCRATCH/mimic-on-fhir-delta/spark_warehouse'))),
        help='Directory holding the generated FHIR Delta tables.')
    parser.add_argument(
        '--out-dir', type=Path,
        default=Path(os.environ.get('OUT_DIR', 'results')),
        help='Directory for the report and CSV artefacts.')
    parser.add_argument(
        '--threads', type=int,
        default=int(os.environ.get('DUCKDB_THREADS', '8')),
        help='DuckDB worker threads.')
    parser.add_argument(
        '--memory-limit', default=os.environ.get('DUCKDB_MEMORY_LIMIT', '48GB'),
        help='DuckDB memory limit, e.g. 48GB.')
    parser.add_argument(
        '--temp-dir', type=Path, default=None,
        help='DuckDB spill directory. Defaults to <out-dir>/duckdb_tmp.')
    parser.add_argument(
        '--sample-limit', type=int, default=50,
        help='Divergent rows to print in the report. The CSV is never capped.')
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    admissions_csv = args.mimic_root / 'hosp' / 'admissions.csv.gz'
    encounter_dir = args.fhir_warehouse / 'Encounter.parquet'

    if not admissions_csv.is_file():
        print(f'ERROR: admissions not found: {admissions_csv}', file=sys.stderr)
        return 2
    if not encounter_dir.is_dir():
        print(f'ERROR: Encounter table not found: {encounter_dir}',
              file=sys.stderr)
        return 2

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = args.temp_dir or (out_dir / 'duckdb_tmp')
    temp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f'SET threads = {int(args.threads)}')
    con.execute(f'SET memory_limit = {sql_literal(args.memory_limit)}')
    con.execute(f'SET temp_directory = {sql_literal(temp_dir)}')
    # Nothing below uses TIMESTAMPTZ. This is set only so that an accidental
    # future TIMESTAMPTZ cast cannot silently pick up the node's local zone.
    con.execute("SET TimeZone = 'UTC'")

    print(f'DuckDB {duckdb.__version__}, threads={args.threads}, '
          f'memory_limit={args.memory_limit}', flush=True)
    print(f'Resolving Delta snapshot: {encounter_dir}', flush=True)

    try:
        snapshot = resolve_snapshot(con, encounter_dir)
        assert_readable_protocol(snapshot)
    except SnapshotError as error:
        print(f'ERROR: {error}', file=sys.stderr)
        return 3

    print(f'  version {snapshot.version}, {len(snapshot.files)} active file(s)',
          flush=True)
    for note in snapshot.notes:
        print(f'  {note}', flush=True)

    print('Building views and running comparison...', flush=True)
    build_views(con, admissions_csv, snapshot)
    results = collect_results(con, out_dir, args.sample_limit)

    config = {
        'admissions': admissions_csv,
        'encounter_table': encounter_dir,
        'hospital_identifier_system': HOSPITAL_IDENTIFIER_SYSTEM,
        'out_dir': out_dir.resolve(),
        'slurm_job_id': os.environ.get('SLURM_JOB_ID', '(not a Slurm job)'),
    }

    report = render_report(results, snapshot, config)
    print()
    print(report, flush=True)

    (out_dir / 'report.txt').write_text(report, encoding='utf-8')

    machine = {
        'generated_utc': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'config': {k: str(v) for k, v in config.items()},
        'snapshot': {
            'table_dir': str(snapshot.table_dir),
            'version': snapshot.version,
            'checkpoint_version': snapshot.checkpoint_version,
            'commits_applied': snapshot.commits_applied,
            'active_file_count': len(snapshot.files),
            'protocol': snapshot.protocol,
            'notes': snapshot.notes,
        },
        'results': {
            key: value for key, value in results.items()
            if key not in {
                'delta_hours_distribution',
                'divergent_source_hour_histogram',
                'divergent_day_names',
                'divergent_sample',
            }
        },
        'delta_hours_distribution': [
            {'delta_hours': d, 'row_count': c}
            for d, c in results['delta_hours_distribution']
        ],
        'divergent_source_hour_histogram': [
            {'oracle_hour': h, 'row_count': c}
            for h, c in results['divergent_source_hour_histogram']
        ],
    }
    (out_dir / 'summary.json').write_text(
        json.dumps(machine, indent=2, default=str), encoding='utf-8')

    print(f'Artefacts written to {out_dir.resolve()}', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
