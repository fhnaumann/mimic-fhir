#!/usr/bin/env python3
"""Build the four-way artifact size comparison table for BENCHMARK.md §7.

Inputs are TSVs of (uncompressed_bytes, filename) produced on the cluster by
decompressing every file, plus the compressed sizes from stat. Both numbers are
needed: comparing a compressed artifact against an uncompressed one measures the
compression setting, not the pipeline, and that distinction was the whole point
of the exercise.

    python3 metrics/artifact_sizes.py
"""
import pathlib
import sys

M = pathlib.Path(__file__).parent


def read_raw(p: pathlib.Path) -> dict:
    """TSV of 'uncompressed_bytes<TAB>filename' -> {resource_stem: bytes}."""
    out = {}
    for line in p.read_text().splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        n, name = parts[0], parts[-1]
        stem = name.replace(".ndjson.gz", "").replace(".ndjson.zst", "") \
                   .replace(".ndjson", "")
        out[stem] = int(n)
    return out


def read_sizes(p: pathlib.Path) -> dict:
    """TSV of 'compressed_bytes<TAB>filename'."""
    return read_raw(p)


def fmt(n) -> str:
    return f"{n:,}" if isinstance(n, int) else str(n)


def gib(n) -> str:
    return f"{n / 2**30:.2f}"


def main() -> int:
    need = ["raw-ref.tsv", "raw-gzip-par.tsv", "raw-zstd.tsv",
            "size-ref.tsv", "size-gzip-par.tsv", "size-zstd.tsv",
            "size-psql-gz.tsv", "size-psql-raw.tsv"]
    missing = [n for n in need if not (M / n).exists()]
    if missing:
        print("missing inputs:", ", ".join(missing), file=sys.stderr)
        return 2

    raw = {"ref": read_raw(M / "raw-ref.tsv"),
           "gz": read_raw(M / "raw-gzip-par.tsv"),
           "zs": read_raw(M / "raw-zstd.tsv")}
    comp = {"ref": read_sizes(M / "size-ref.tsv"),
            "gz": read_sizes(M / "size-gzip-par.tsv"),
            "zs": read_sizes(M / "size-zstd.tsv"),
            "pgz": read_sizes(M / "size-psql-gz.tsv")}
    praw = read_sizes(M / "size-psql-raw.tsv")

    names = sorted(praw)
    print("| resource | psql raw | psql .gz | published .gz | DuckDB .gz | DuckDB .zst |")
    print("|---|---:|---:|---:|---:|---:|")
    tot = {k: 0 for k in ("praw", "pgz", "ref", "gz", "zs")}
    for n in names:
        row = (praw.get(n, 0), comp["pgz"].get(n, 0), comp["ref"].get(n, 0),
               comp["gz"].get(n, 0), comp["zs"].get(n, 0))
        for k, v in zip(("praw", "pgz", "ref", "gz", "zs"), row):
            tot[k] += v
        print(f"| `{n}` | " + " | ".join(fmt(v) for v in row) + " |")
    print("| **total** | " + " | ".join(
        f"**{fmt(tot[k])}**" for k in ("praw", "pgz", "ref", "gz", "zs")) + " |")
    print("| **GiB** | " + " | ".join(
        f"**{gib(tot[k])}**" for k in ("praw", "pgz", "ref", "gz", "zs")) + " |")

    print("\n--- uncompressed totals (content size, compression removed) ---")
    for k, label in (("ref", "published"), ("gz", "DuckDB gzip"),
                     ("zs", "DuckDB zstd")):
        t = sum(raw[k].values())
        print(f"{label:<14} {t:,}  ({gib(t)} GiB)  "
              f"{100*t/tot['praw']:.1f}% of psql raw")
    print(f"{'psql':<14} {tot['praw']:,}  ({gib(tot['praw'])} GiB)  100.0%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
