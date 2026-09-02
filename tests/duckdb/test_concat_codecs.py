#!/usr/bin/env python3
"""Every codec in driver.CONCATENABLE must survive PER_THREAD_OUTPUT + byte join.

driver.export() writes one part per thread and concatenates them into a single
file, which is only valid for container formats defined as a SEQUENCE of
independent members/frames. gzip and zstd both are. This asserts it rather than
citing the specs, because the failure mode is quiet:

    a reader that stops after the first member returns FEWER rows, every one of
    them correct, from a file that opens without error.

Nothing about that looks like a bug until parity runs hours later, so both a
DuckDB read-back and the codec's own CLI tool have to agree on the full count.
The CLI check matters independently: the export is a published artifact, and a
file only DuckDB can decompress is not really an `.ndjson.gz`.

REQUIRES A REAL MULTI-CORE MACHINE. DuckDB emits one part per thread that
actually wrote rows, so on a 1-core box (the HPC login node, for one) this
produces a single part and asserts nothing. The test fails loudly in that case
rather than passing vacuously -- run it inside an allocation:

    sbatch --cpus-per-task=8 --mem=16G --time=00:20:00 \
           --wrap "venv/bin/python tests/duckdb/test_concat_codecs.py"
"""
import pathlib
import shutil
import subprocess
import sys
import tempfile

import duckdb

N = 20_000_000          # large enough to spread across threads
THREADS = 8
CLI = {"gzip": ["gzip", "-dc"], "zstd": ["zstd", "-dc"]}

# Imported rather than duplicated: the point is to test what the driver ships.
sys.path.insert(
    0, str(pathlib.Path(__file__).resolve().parents[2] / "duckdb_etl"))
from driver import CONCATENABLE, COMPRESSION          # noqa: E402

BASE_OPTS = ["FORMAT csv", "HEADER false", r"QUOTE e'\t'", r"ESCAPE e'\t'",
             r"DELIMITER e'\x01'"]


def check(con, codec: str, tmp: pathlib.Path, fails: list) -> None:
    keyword, suffix = COMPRESSION[codec]
    opts = BASE_OPTS + ([f"COMPRESSION {keyword}"] if keyword else [])

    part_dir = tmp / f"parts{suffix}"
    con.execute(f"COPY (SELECT fhir FROM v) TO '{part_dir}' "
                f"({', '.join(opts)}, PER_THREAD_OUTPUT true)")
    parts = sorted(part_dir.iterdir())

    joined = tmp / f"joined{suffix}"
    with open(joined, "wb") as out:
        for p in parts:
            with open(p, "rb") as fh:
                shutil.copyfileobj(fh, out, 8 << 20)

    single = tmp / f"single{suffix}"
    con.execute(f"COPY (SELECT fhir FROM v) TO '{single}' ({', '.join(opts)})")

    print(f"\n{codec}: {len(parts)} parts  joined={joined.stat().st_size:,} B  "
          f"single={single.stat().st_size:,} B")

    if len(parts) < 2:
        fails.append(f"{codec}: only {len(parts)} part written -- this machine "
                     f"cannot exercise concatenation, the result proves nothing")
        print("  ! ONLY ONE PART -- test is vacuous here, need a multi-core node")
        return

    n_join = con.execute(
        f"SELECT count(*) FROM read_ndjson_objects('{joined}')").fetchone()[0]
    n_single = con.execute(
        f"SELECT count(*) FROM read_ndjson_objects('{single}')").fetchone()[0]
    # Count DISTINCT ids too: a reader that silently repeated one frame would
    # satisfy a bare count(*) but not this.
    ids = con.execute(f"SELECT count(DISTINCT json_extract_string(j, '$.id')) "
                      f"FROM read_ndjson_objects('{joined}') t(j)").fetchone()[0]

    for label, got in (("duckdb rows", n_join), ("distinct ids", ids),
                       ("single-stream rows", n_single)):
        ok = got == N
        print(f"  {label:<20} {got:>12,}  want {N:,}   {'ok' if ok else 'FAIL'}")
        if not ok:
            fails.append(f"{codec}: {label} = {got:,}, want {N:,}")

    tool = CLI.get(codec)
    if not tool:
        fails.append(f"{codec}: no CLI cross-check configured -- add one to CLI")
        return
    try:
        r = subprocess.run(tool + [str(joined)], capture_output=True)
    except FileNotFoundError:
        fails.append(f"{codec}: {tool[0]} not installed, CLI cross-check skipped "
                     f"-- the artifact's external readability is UNVERIFIED")
        print(f"  {tool[0]:<20} not installed   FAIL (cannot verify)")
        return
    if r.returncode != 0:
        fails.append(f"{codec}: {tool[0]} exit {r.returncode}: "
                     f"{r.stderr.decode()[:200]}")
        print(f"  {tool[0]:<20} rc={r.returncode}   FAIL")
        return
    lines = r.stdout.count(b"\n")
    ok = lines == N
    print(f"  {tool[0] + ' -dc':<20} {lines:>12,}  want {N:,}   "
          f"{'ok' if ok else 'FAIL'}")
    if not ok:
        fails.append(f"{codec}: {tool[0]} -dc yielded {lines:,} lines, want {N:,}")


def main() -> int:
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="concat-codecs-"))
    con = duckdb.connect()
    print("duckdb:", con.execute("select version()").fetchone()[0])
    con.execute(f"SET threads={THREADS}")
    con.execute("SET preserve_insertion_order=false")
    # A TABLE, not a view over range(). range() is a single-threaded table
    # function, so a view over it feeds the writer from one thread and
    # PER_THREAD_OUTPUT emits exactly one part -- the test then passes while
    # exercising nothing. A materialised table scans in parallel, which is also
    # what the real build does (it reads Parquet). Verified by the >= 2 parts
    # assertion in check(); do not "simplify" this back to a view.
    con.execute(f"""CREATE TABLE v AS SELECT to_json(
        {{'id': i::VARCHAR, 'n': i, 'pad': repeat('x', 60)}})::VARCHAR AS fhir
        FROM range({N}) t(i)""")

    fails: list = []
    try:
        for codec in sorted(CONCATENABLE):
            check(con, codec, tmp, fails)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if fails:
        print(f"{len(fails)} FAILURE(S):")
        for f in fails:
            print("  -", f)
        return 1
    print(f"all {len(CONCATENABLE)} concatenable codec(s) verified: "
          f"{', '.join(sorted(CONCATENABLE))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
