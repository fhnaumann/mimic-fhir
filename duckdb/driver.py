#!/usr/bin/env python3
"""Build the MIMIC-IV-on-FHIR ndjson export with DuckDB.

    python duckdb/driver.py --parquet /tmp/mimic-parquet --out /tmp/fhir-out

Consumes what the other two tools produce -- `build/duckdb/manifest.json` from
transpile.py and `stage_manifest.json` from stage.py -- and owns everything that
is a deployment choice rather than a mapping choice: session settings, load order,
and I/O policy.

I/O policy lives here and NOT in the generated SQL. transpile.py emits
`CREATE OR REPLACE TEMP VIEW v_x AS SELECT ...`, and this file decides what to do
with each view: write ndjson (the build), or materialise `mimic_fhir.x` as a real
table for a subject subset (the HAPI validation path, which queries Postgres
directly -- see HANDOFF.md 2). Baking `COPY` into generated SQL would hardcode the
compression into the transpiler output and make the gzip/zstd runs differ by more
than one setting.

THE LOAD ORDER IS LOad-BEARING, in one specific way that produces a confusing
error. `fn_patient_extension` resolves `fhir_etl.map_ethnicity` and
`fhir_etl.map_race_omb` at CREATE MACRO time, not at call time, so 20_functions.sql
must come after the fhir_etl lookup tables. Loading it early gives
`Catalog Error: Table with name "fhir_etl.map_ethnicity" does not exist` pointing
at a macro definition that looks completely fine.

The ndjson writer borrows psql's trick but NOT psql's characters -- copying those
verbatim silently corrupts 12% of some resources. See export() for why, and for the
read-back check that makes it a build failure rather than a parity finding.

Per-resource write time and byte counts are recorded from the first run. They
cannot be reconstructed afterwards, and they are what turns the zstd comparison
into a 30-row table instead of two wall-clock numbers.

Usage:
    python duckdb/driver.py --parquet DIR --out DIR
    python duckdb/driver.py --parquet DIR --out DIR --only patient,specimen
    python duckdb/driver.py --parquet DIR --out DIR --compression none
    python duckdb/driver.py --parquet DIR --db build.duckdb --materialize 20
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import shutil
import sys
import time

REPO = pathlib.Path(__file__).resolve().parent.parent
RUNTIME = REPO / "duckdb" / "runtime"
BUILD = REPO / "build" / "duckdb"

SETTINGS = RUNTIME / "00_settings.sql"
MACROS = RUNTIME / "10_macros.sql"
FUNCTIONS = RUNTIME / "20_functions.sql"
SELFTEST = RUNTIME / "99_selftest.sql"

SCHEMAS = ["fhir_etl", "fhir_trm", "mimic_fhir"]

# Compression -> (DuckDB COPY keyword, filename suffix). gzip is the default and
# stays the default: zstd is a separate run so the benchmark delta is clean, and
# it is never a per-resource choice.
COMPRESSION = {
    "none": (None, ""),
    "gzip": ("gzip", ".gz"),
    "zstd": ("zstd", ".zst"),
}

# Codecs whose on-disk format is a sequence of independent members/frames, so
# byte-concatenating PER_THREAD_OUTPUT parts yields one valid file. Verified by
# tests/test_concat_codecs.py against BOTH DuckDB's reader and the codec's own
# CLI tool. Anything not listed here falls back to a single compressor thread.
CONCATENABLE = frozenset({"gzip", "zstd"})


def substitute(text: str, subs: dict[str, str], where: str) -> str:
    """Replace ${NAME} placeholders, and fail loudly on any that is left over."""
    for k, v in subs.items():
        text = text.replace("${" + k + "}", str(v))
    left = re.findall(r"\$\{(\w+)\}", text)
    if left:
        raise SystemExit(f"{where}: unsubstituted placeholder(s) {sorted(set(left))}")
    return text


def source_views(con, parquet_root: pathlib.Path) -> int:
    """Expose the staged Parquet as mimiciv_* views, the shape sql/ expects.

    Views rather than tables: DuckDB reads Parquet directly and a scan of a view
    over read_parquet is the same plan as a scan of a table, without paying to
    load 300 M rows into the database first.
    """
    manifest = parquet_root / "stage_manifest.json"
    if not manifest.exists():
        raise SystemExit(f"no stage_manifest.json under {parquet_root} -- run "
                         f"duckdb/stage.py first")
    tables = json.loads(manifest.read_text())["tables"]
    for schema in sorted({t.split(".")[0] for t in tables}):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for table, rec in sorted(tables.items()):
        path = pathlib.Path(rec["parquet"])
        if not path.is_absolute():
            path = parquet_root / path
        con.execute(f"CREATE OR REPLACE VIEW {table} AS "
                    f"SELECT * FROM read_parquet('{path}')")
    return len(tables)


def load_order(includes: list[str]) -> list[tuple[str, pathlib.Path]]:
    """(label, path) in the order everything must be executed.

    `includes` is psql's own \\i order, straight out of the manifest, and is not
    reordered here -- the codesystem tables have to exist before the resource views
    that select from them, because DuckDB binds a view body at CREATE time rather
    than at first use.
    """
    etl = [f for f in includes if f.startswith("fhir_etl/")]
    rest = [f for f in includes if not f.startswith("fhir_etl/")]

    order: list[tuple[str, pathlib.Path]] = [("runtime/10_macros.sql", MACROS)]
    order += [(f, BUILD / f) for f in etl]
    # After the lookup tables, before anything that calls a fn_*.
    order.append(("runtime/20_functions.sql", FUNCTIONS))
    order.append(("runtime/99_selftest.sql", SELFTEST))
    order += [(f, BUILD / f) for f in rest]
    return order


def export(con, view: str, filename: str, out_dir: pathlib.Path,
           compression: str, verify: bool = True, parallel: bool = True) -> dict:
    r"""Write one resource as ndjson and measure it.

    THE QUOTE CHARACTER IS A TAB, AND THAT IS THE WHOLE TRICK. psql exports with
    `QUOTE AS E'\b' DELIMITER E'\001'`, picking characters that cannot occur in the
    JSON so its CSV writer never quotes anything. Copying that choice here produces
    a BROKEN file, because DuckDB's CSV writer also quotes any value containing the
    reader's comment character `#` -- which is hardcoded; COMMENT '' on the COPY is
    accepted and ignored. On the demo alone that quoted 1,173 of 9,362
    Observation-outputevents lines (`Chest Tube #1`, `Tube #2`), each emitted as
    \b{""resourceType"":...}\b -- valid CSV, not valid JSON, and only 12% of lines,
    so it survives a spot check.

    A tab sidesteps it rather than fighting it:
      * DuckDB's JSON serialiser always escapes a literal tab as \t, so the value
        can never contain the quote character and nothing is ever doubled;
      * a leading and trailing tab is legal JSON whitespace, so a quoted line
        parses identically to an unquoted one.
    So both the quoted and unquoted forms are correct ndjson, and no post-processing
    is needed. Verified below rather than assumed.

    PARALLEL COMPRESSION. A single `COPY ... TO 'one.gz'` runs ONE compressor
    thread no matter how many are allocated. Measured on the full extract: 1.0-2.0
    cores busy out of 16 for the whole export, with brief 4-9 core bursts only at
    resource boundaries -- the scan parallelises, the gzip stream cannot. That is
    the single biggest reason a build underuses the node.

    PER_THREAD_OUTPUT gives each thread its own file and so its own compressor.
    That would normally change the output layout to a directory of parts, which
    would ripple into every consumer -- but gzip (multi-member) and zstd
    (multi-frame) both define their container as a SEQUENCE of independent units,
    so concatenating the parts byte-wise yields ONE valid file that decompresses
    to the concatenation. Verified for both codecs against DuckDB's reader and the
    codec's own CLI: tests/test_concat_codecs.py.

    So the layout is unchanged (one .ndjson.gz / .ndjson.zst per resource, same
    shape as psql's) and the compression is spread across all threads. The join is
    a byte copy at disk speed. Set parallel=False to get the old single-stream
    behaviour for a like-for-like benchmark against psql.

    Do NOT extend CONCATENABLE by inspection of a format spec. The failure mode is
    a reader that stops after the first unit: it returns fewer rows, every one of
    them correct, from a file that opens cleanly. The row-count verify below is
    what catches it, and only because it re-reads the joined file rather than
    trusting the write.
    """
    keyword, suffix = COMPRESSION[compression]
    dest = out_dir / (filename + suffix)
    opts = ["FORMAT csv", "HEADER false", r"QUOTE e'\t'", r"ESCAPE e'\t'",
            r"DELIMITER e'\x01'"]
    if keyword:
        opts.append(f"COMPRESSION {keyword}")

    # Concatenation is only valid for codecs whose format is a sequence of
    # independent members/frames. gzip (multi-member) and zstd (multi-frame) both
    # are, and both are verified end to end -- DuckDB's reader AND the standard
    # command-line tool must return every row, because a reader that stops after
    # the first frame silently returns a smaller, perfectly valid-looking file.
    # See tests/test_concat_codecs.py; do not add a codec here without adding it
    # there. `none` is excluded: raw CSV parts concatenate fine, but there is no
    # compressor to parallelise, so PER_THREAD_OUTPUT would only add a copy.
    parallel_ok = parallel and compression in CONCATENABLE

    t0 = time.time()
    if parallel_ok:
        part_dir = out_dir / f".{filename}.parts"
        if part_dir.exists():
            shutil.rmtree(part_dir)
        con.execute(f"COPY (SELECT fhir FROM {view}) TO '{part_dir}' "
                    f"({', '.join(opts)}, PER_THREAD_OUTPUT true)")
        parts = sorted(part_dir.iterdir())
        if not parts:
            raise SystemExit(f"{filename}: PER_THREAD_OUTPUT wrote no parts")
        with open(dest, "wb") as out:
            for part in parts:
                with open(part, "rb") as fh:
                    shutil.copyfileobj(fh, out, 8 << 20)
        shutil.rmtree(part_dir)
    else:
        parts = []
        con.execute(f"COPY (SELECT fhir FROM {view}) TO '{dest}' "
                    f"({', '.join(opts)})")
    elapsed = time.time() - t0
    rows = con.execute(f"SELECT count(*) FROM {view}").fetchone()[0]

    rec = {"view": view, "file": dest.name, "rows": rows,
           "seconds": round(elapsed, 2), "bytes": dest.stat().st_size,
           "parts": len(parts)}
    if verify:
        # Read the file back as ndjson. This is the check that would have caught
        # the quoting bug above at write time instead of at parity time, and it
        # costs a parse rather than a rebuild. read_ndjson_objects does no schema
        # inference, so it is checking framing -- one JSON value per line -- which
        # is exactly the property the CSV writer can break.
        t1 = time.time()
        got = con.execute(
            f"SELECT count(*) FROM read_ndjson_objects('{dest}')").fetchone()[0]
        rec["verify_seconds"] = round(time.time() - t1, 2)
        if got != rows:
            raise SystemExit(f"{dest.name}: wrote {rows:,} rows but only {got:,} "
                             f"lines parse as ndjson")
    return rec


def materialize(con, views: dict[str, str], n: int) -> list[str]:
    """Real mimic_fhir.* tables for a subject subset, for the HAPI validation path.

    py_mimic_fhir/db.py queries `mimic_fhir.{table}` directly (HANDOFF.md 2), so
    those tables have to exist -- but validation only ever samples patients, so
    this is deliberately capped rather than materialising 313 M rows.
    """
    subs = [r[0] for r in con.execute(
        "SELECT DISTINCT patient_id FROM v_patient LIMIT ?", [n]).fetchall()]
    if not subs:
        return []
    lit = ", ".join(f"'{s}'" for s in subs)
    made = []
    for table, view in sorted(views.items()):
        con.execute(f"CREATE OR REPLACE TABLE mimic_fhir.{table} AS "
                    f"SELECT * FROM {view} WHERE patient_id IN ({lit})")
        made.append(table)
    return made


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--parquet", required=True, help="stage.py output root")
    ap.add_argument("--out", help="ndjson output directory (omit to build only)")
    ap.add_argument("--db", help="persist the database here (needed for --materialize)")
    ap.add_argument("--build", default=str(BUILD), help="transpile.py output root")
    ap.add_argument("--compression", default="gzip", choices=sorted(COMPRESSION))
    ap.add_argument("--only", help="comma-separated resource names to export")
    ap.add_argument("--materialize", type=int, metavar="N",
                    help="also create mimic_fhir.* tables for N patients")
    ap.add_argument("--threads", type=int,
                    default=int(os.environ.get("SLURM_CPUS_PER_TASK") or os.cpu_count()))
    ap.add_argument("--memory-limit", default=None,
                    help="DuckDB memory_limit. Keep it near 60%% of --mem: the limit "
                         "governs the buffer manager only, and JSON string building "
                         "sits outside the tracked pool (measured 2.4 GB RSS under a "
                         "1 GB limit)")
    ap.add_argument("--temp-dir", default=None,
                    help="spill directory; default $TMPDIR, then ./duckdb-tmp")
    ap.add_argument("--no-verify", action="store_true",
                    help="skip reading each export back as ndjson. The read-back is "
                         "what catches a CSV-writer quoting bug at write time; only "
                         "skip it if you are re-running a known-good configuration")
    ap.add_argument("--single-stream", action="store_true",
                    help="one compressor thread per resource, as psql does. The "
                         "default spreads compression across all threads via "
                         "PER_THREAD_OUTPUT and concatenates the gzip members, "
                         "which is the same output but uses the node. Use this "
                         "for a like-for-like benchmark against the baseline")
    ap.add_argument("--metrics", help="write per-resource timings here as JSON")
    args = ap.parse_args()

    try:
        import duckdb
    except ImportError:
        raise SystemExit("duckdb is required: pip install duckdb")
    if not hasattr(duckdb, "connect"):
        raise SystemExit("`import duckdb` found this repo's duckdb/ namespace package "
                         "instead of the PyPI module (HANDOFF.md trap 16) -- run from "
                         "outside the repo root or install duckdb")

    build = pathlib.Path(args.build)
    manifest_path = build / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"no manifest at {manifest_path} -- run duckdb/transpile.py")
    manifest = json.loads(manifest_path.read_text())

    temp_dir = args.temp_dir or os.environ.get("TMPDIR") or str(pathlib.Path.cwd()
                                                               / "duckdb-tmp")
    pathlib.Path(temp_dir).mkdir(parents=True, exist_ok=True)

    mem = args.memory_limit
    if mem is None:
        slurm = os.environ.get("SLURM_MEM_PER_NODE")   # MB, if Slurm set it
        mem = f"{int(int(slurm) * 0.6)}MB" if slurm else "8GB"

    con = duckdb.connect(args.db or ":memory:")
    con.execute(substitute(SETTINGS.read_text(),
                           {"THREADS": args.threads, "MEMORY_LIMIT": mem,
                            "TEMP_DIR": temp_dir}, str(SETTINGS)))
    print(f"settings:  threads={args.threads} memory_limit={mem} temp_dir={temp_dir}")

    for s in SCHEMAS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
    n_src = source_views(con, pathlib.Path(args.parquet).resolve())
    print(f"sources:   {n_src} mimiciv_* views over Parquet")

    steps = load_order(manifest["include_order"])
    t0 = time.time()
    for label, path in steps:
        if not path.exists():
            raise SystemExit(f"missing {label} at {path} -- re-run duckdb/transpile.py")
        try:
            # Each FILE is one unit. 22 of the 109 source files do not end in a
            # semicolon and rely on psql flushing at EOF, so anything that
            # concatenates files or splits on ';' produces syntax errors.
            con.execute(path.read_text())
        except Exception as e:
            raise SystemExit(f"\n{label}\n  {type(e).__name__}: {e}")
    print(f"loaded:    {len(steps)} files in {time.time() - t0:.1f}s "
          f"(selftest passed)")

    views = manifest["views"]
    outputs = manifest["output_files"]
    if args.only:
        keep = {o.strip().lower() for o in args.only.split(",")}
        views = {t: v for t, v in views.items() if t.lower() in keep}
        unknown = keep - {t.lower() for t in manifest["views"]}
        if unknown:
            raise SystemExit(f"unknown resource(s): {sorted(unknown)}\n"
                             f"known: {', '.join(sorted(manifest['views']))}")

    records = []
    if args.out:
        out_dir = pathlib.Path(args.out).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        print(f"\nexport:    {out_dir}  compression={args.compression}")
        print(f"  {'resource':<34} {'rows':>12}  {'MB':>9}  {'sec':>7}  {'MB/s':>7}")
        for table, view in sorted(views.items()):
            rec = export(con, view, outputs[view], out_dir, args.compression,
                         verify=not args.no_verify,
                         parallel=not args.single_stream)
            rec["table"] = table
            records.append(rec)
            mb = rec["bytes"] / 1e6
            rate = mb / rec["seconds"] if rec["seconds"] else float("nan")
            print(f"  {table:<34} {rec['rows']:>12,}  {mb:>9.1f}  "
                  f"{rec['seconds']:>7.1f}  {rate:>7.1f}", flush=True)

    if args.materialize:
        if not args.db:
            raise SystemExit("--materialize needs --db: the tables must outlive the "
                             "process for py_mimic_fhir to query them")
        made = materialize(con, manifest["views"], args.materialize)
        print(f"\nmaterialised {len(made)} mimic_fhir.* tables "
              f"for up to {args.materialize} patients")

    if records:
        total_s = sum(r["seconds"] for r in records)
        total_b = sum(r["bytes"] for r in records)
        total_r = sum(r["rows"] for r in records)
        print(f"\n{len(records)} resources, {total_r:,} rows, "
              f"{total_b / 1e9:.2f} GB, {total_s / 60:.1f} min of write time")
        if args.metrics:
            pathlib.Path(args.metrics).write_text(json.dumps({
                "compression": args.compression, "threads": args.threads,
                "memory_limit": mem, "resources": records,
                "total_rows": total_r, "total_bytes": total_b,
                "total_write_seconds": round(total_s, 1),
            }, indent=2))
            print(f"metrics:   {args.metrics}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
