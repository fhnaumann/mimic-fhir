#!/usr/bin/env python3
"""Semantic ndjson differ for the DuckDB port — compares a DuckDB export against psql.

    python duckdb/parity.py $SCRATCH/psql-baseline/out build/ndjson

Neither export is byte-comparable, and the differences that make it so are all
uninteresting. This normalises exactly those and nothing else:

  * OBJECT KEY ORDER. Postgres `jsonb` sorts keys length-then-bytes; DuckDB `JSON`
    preserves insertion order. Same document, different bytes, every single line.
  * NUMERIC FORMATTING. `fhir_decimal` is CAST AS DOUBLE, so Postgres `5.20` comes
    back as `5.2` and `7` as `7.0`. Same FHIR decimal. Numbers are parsed as
    `decimal.Decimal` and compared by value, so 5.20 == 5.2 and 80 == 80.0.
  * LINE ORDER. DuckDB runs with `preserve_insertion_order = false`, so row order
    is not stable between runs let alone between backends. Resources are matched
    by `id`, not by position.

Nothing else is normalised. A missing key, a null that should be absent, a
truncated decimal or a reformatted timestamp all show up as diffs, which is the
entire point — those are the failure modes the port actually has (see the traps
table in HANDOFF.md §7).

The report is a JSON-PATH HISTOGRAM, not a list of diffs. A systematic rewriter
bug is one path with 40,000 hits; the same information as 40,000 individual diff
records is unreadable and buries the second, rarer bug underneath it. Array
indices collapse to `[*]` in the histogram so that per-element noise aggregates.

SCALE. Two strategies, chosen automatically by input size (see `compare`).

Under the threshold, both sides are indexed in memory as raw lines (not parsed
objects): ~3x the file size in RSS, ~75k resources/s -- measured, 300k rows /
131 MB in 4.0 s at 380 MB RSS, with every line byte-different so no fast path
fires. Comfortable for the 803 MB demo export.

Over it, side A is reduced to a dict of id -> 16-byte digest of the CANONICAL
form, side B is streamed and digested the same way, and only the ids that
actually differ are re-read in a second pass to build the histogram. That trades
holding ~600 GB of text for ~50 GB of digests on the 313 M-row chartevents, at
the cost of reading both files twice when there is anything to report.

Both strategies are asserted to agree, field for field, on every case in
tests/test_parity.py.

Exit status: 0 identical, 1 differences found, 2 could not run.

Usage:
    python duckdb/parity.py DIR_A DIR_B
    python duckdb/parity.py DIR_A DIR_B --only MimicPatient --max-examples 5
    python duckdb/parity.py A.ndjson B.ndjson.gz          # single file pair
    python duckdb/parity.py DIR_A DIR_B --json report.json
    python duckdb/parity.py DIR_A DIR_B --ignore '$.meta.lastUpdated'
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import pathlib
import re
import sys
from collections import Counter
from decimal import Decimal

# Both sides go through parse_int too, not just parse_float. Otherwise one side
# yields int and the other Decimal for the same value; they compare equal, but
# they do not canonicalise equal, and the array multiset check below is exactly
# where that bites.
_PARSE = dict(parse_float=Decimal, parse_int=Decimal)

MAX_VALUE_REPR = 90


# --------------------------------------------------------------------------
# JSON value helpers
# --------------------------------------------------------------------------

def jtype(v) -> str:
    """JSON type name. Checks bool before number -- bool is a subclass of int."""
    if v is None:
        return "null"
    if v is True or v is False:
        return "bool"
    if isinstance(v, (Decimal, int, float)):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, dict):
        return "object"
    if isinstance(v, list):
        return "array"
    return type(v).__name__


def num_str(d: Decimal) -> str:
    """Canonical decimal string: 80, 80.0 and 8E+1 all render as '80'.

    normalize() alone is not enough -- it turns Decimal('80.0') into Decimal('8E+1').
    Formatting with 'f' expands the exponent back out, which is what makes the
    result usable as a dictionary key.
    """
    try:
        return format(Decimal(d).normalize(), "f")
    except Exception:                                          # pragma: no cover
        return str(d)


def canon(v) -> str:
    """Order-independent, format-independent serialisation, for use as a key.

    Numbers carry a sentinel prefix so that the number 80 and the string "80"
    cannot collide in the array multiset comparison.
    """
    return json.dumps(v, sort_keys=True, separators=(",", ":"),
                      default=lambda o: "\x00num:" + num_str(o))


def brief(v) -> str:
    s = canon(v) if isinstance(v, (dict, list)) else (
        num_str(v) if isinstance(v, Decimal) else json.dumps(v))
    return s if len(s) <= MAX_VALUE_REPR else s[:MAX_VALUE_REPR - 1] + "…"


# --------------------------------------------------------------------------
# Diff
#
# Emits (kind, path, detail) triples. `kind` is what is wrong, `path` is where.
# Kinds, and what each one usually means here:
#
#   only_a / only_b     a key present on one side only. The common cause is
#                       strip() placement -- rule 1b wraps every json_object, so
#                       a key surviving on the DuckDB side means an object that
#                       is not a json_object node (a CASE, a fn_* return).
#   value               same type, different value. Timestamp formatting and
#                       DECIMAL(18,3) truncation both land here.
#   type                different JSON type. A number rendered as a string is
#                       the json_each pivot leaking (trap 5 / HANDOFF §9.1).
#   array_length        different element count. jsonb_strip_nulls does not drop
#                       null array ELEMENTS on either side, so this is real.
#   array_order         same elements, different order -- an aggregate missing an
#                       ORDER BY. Usually cosmetic, but flagged separately rather
#                       than silently accepted, because FHIR array order is
#                       meaningful in some fields (name, telecom by rank).
#   array_elem_only_a/b element multiset delta, reported shallow: recursing into
#                       length-mismatched arrays produces a cascade of noise that
#                       hides the one real difference.
# --------------------------------------------------------------------------

GENERIC_INDEX = re.compile(r"\[\d+\]")


def generic(path: str) -> str:
    return GENERIC_INDEX.sub("[*]", path)


def diff(a, b, path: str, out: list) -> None:
    ta, tb = jtype(a), jtype(b)
    if ta != tb:
        out.append(("type", path, f"{ta}={brief(a)} vs {tb}={brief(b)}"))
        return

    if ta == "object":
        for k in a:
            if k not in b:
                out.append(("only_a", f"{path}.{k}", brief(a[k])))
        for k in b:
            if k not in a:
                out.append(("only_b", f"{path}.{k}", brief(b[k])))
            else:
                diff(a[k], b[k], f"{path}.{k}", out)
        return

    if ta == "array":
        diff_array(a, b, path, out)
        return

    if a != b:
        out.append(("value", path, f"{brief(a)} vs {brief(b)}"))


def diff_array(a: list, b: list, path: str, out: list) -> None:
    if len(a) != len(b):
        out.append(("array_length", path, f"{len(a)} vs {len(b)}"))
        ca, cb = Counter(canon(x) for x in a), Counter(canon(x) for x in b)
        for x in (ca - cb):
            out.append(("array_elem_only_a", f"{path}[*]", brief_canon(x)))
        for x in (cb - ca):
            out.append(("array_elem_only_b", f"{path}[*]", brief_canon(x)))
        return

    if a == b:
        return

    # Same length and not equal: distinguish a reordering from a real difference
    # before recursing, or a rotated array reports every element as changed.
    if Counter(canon(x) for x in a) == Counter(canon(x) for x in b):
        out.append(("array_order", path, f"{len(a)} elements reordered"))
        return

    for i, (x, y) in enumerate(zip(a, b)):
        diff(x, y, f"{path}[{i}]", out)


def brief_canon(s: str) -> str:
    s = s.replace("\x00num:", "")
    return s if len(s) <= MAX_VALUE_REPR else s[:MAX_VALUE_REPR - 1] + "…"


# --------------------------------------------------------------------------
# ndjson I/O
# --------------------------------------------------------------------------

def open_ndjson(path: pathlib.Path) -> io.TextIOBase:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def stem(path: pathlib.Path) -> str:
    """MimicPatient.ndjson and MimicPatient.ndjson.gz are the same resource."""
    name = path.name
    if name.endswith(".gz"):
        name = name[:-3]
    for suf in (".ndjson", ".json"):
        if name.endswith(suf):
            return name[:-len(suf)]
    return name


def scan(path: pathlib.Path, side: str, stats: dict):
    """Yield (id, raw_line) and fill `stats`, without holding the file.

    Shared by both comparison strategies so that line counting, parse-error
    reporting and the synthetic-id convention cannot drift between them.
    """
    with open_ndjson(path) as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                stats["blank"] += 1
                continue
            stats["lines"] += 1
            try:
                obj = json.loads(line, **_PARSE)
            except json.JSONDecodeError as e:
                stats["parse_errors"] += 1
                if stats["parse_errors"] <= 3:
                    print(f"  ! {side} {path.name}:{lineno} unparseable: {e}",
                          file=sys.stderr)
                continue
            rid = obj.get("id") if isinstance(obj, dict) else None
            if not isinstance(rid, str):
                stats["missing_id"] += 1
                rid = f"\x00noid:{side}:{lineno}"
            yield rid, line, obj


def new_stats() -> dict:
    return {"lines": 0, "blank": 0, "duplicate_ids": 0, "missing_id": 0,
            "parse_errors": 0}


def index_lines(path: pathlib.Path, side: str) -> tuple[dict, dict]:
    """id -> raw line, plus counters.

    Raw lines rather than parsed objects: the parsed form of a large resource is
    several times the file size in RSS, and only matched pairs are ever parsed
    twice. The full-scale exports are large enough that this matters.
    """
    index: dict[str, str] = {}
    stats = {"lines": 0, "blank": 0, "duplicate_ids": 0, "missing_id": 0,
             "parse_errors": 0}
    dupes: Counter = Counter()

    with open_ndjson(path) as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                stats["blank"] += 1
                continue
            stats["lines"] += 1
            try:
                obj = json.loads(line, **_PARSE)
            except json.JSONDecodeError as e:
                stats["parse_errors"] += 1
                if stats["parse_errors"] <= 3:
                    print(f"  ! {side} {path.name}:{lineno} unparseable: {e}",
                          file=sys.stderr)
                continue
            rid = obj.get("id") if isinstance(obj, dict) else None
            if not isinstance(rid, str):
                stats["missing_id"] += 1
                rid = f"\x00noid:{side}:{lineno}"
            if rid in index:
                stats["duplicate_ids"] += 1
                dupes[rid] += 1
                rid = f"{rid}\x00dup{dupes[rid]}"
            index[rid] = line

    return index, stats


# --------------------------------------------------------------------------
# Per-resource comparison
# --------------------------------------------------------------------------

def uncompressed_size(path: pathlib.Path) -> int:
    """On-disk size, scaled up for .gz so the dispatcher sees comparable numbers.

    A gzipped ndjson export runs about 17x here (chartevents: 15.7 GB gz for
    262 GB of text). 10x is deliberately conservative -- underestimating picks
    the in-memory strategy for something that does not fit in RAM, which is an
    OOM kill hours in; overestimating only picks the slower strategy.
    """
    size = path.stat().st_size
    return size * 10 if path.suffix == ".gz" else size


def compare(path_a: pathlib.Path, path_b: pathlib.Path, args) -> dict:
    """Dispatch on size. Both strategies return the same shape.

    The in-memory one holds every raw line of BOTH sides, which costs roughly
    2.5x the uncompressed size of the pair. That is nothing at demo scale and
    impossible for the two big resources: MimicObservationChartevents is 262 GB
    and 313 M lines, so both sides indexed is ~600 GB against a 505 GB node.

    The larger of the two sides decides. Sizing off A alone happens to work when
    A is the uncompressed psql baseline, but it silently picks the in-memory
    path for a small-A/huge-B pair, which is an OOM rather than an error.
    """
    biggest = max(uncompressed_size(path_a), uncompressed_size(path_b))
    if biggest > args.digest_threshold_gb * (1 << 30):
        return compare_digest(path_a, path_b, args)
    return compare_in_memory(path_a, path_b, args)


def compare_digest(path_a: pathlib.Path, path_b: pathlib.Path, args) -> dict:
    """Two passes: find WHICH records differ, then re-read only those to say HOW.

    Pass 1 keeps a 16-byte digest of each canonicalised record instead of the
    record, which is ~75 GB for chartevents rather than ~300 GB, and streams side
    B against it. Digests are taken over canon(), not the raw line, so key order
    and numeric formatting normalise exactly as they do in the in-memory path --
    which matters, because ZERO raw lines match byte-for-byte between psql and
    DuckDB (psql writes `{"id": ...` with spaces and sorted keys, DuckDB writes
    compact insertion-order JSON).

    Pass 2 re-reads both files keeping only the differing ids, and runs the same
    diff() as the in-memory path to build the histogram. That pass is cheap when
    the answer is "identical" and bounded by --max-detail when it is not.

    16-byte digests, not 8: at 313 M records an 8-byte digest has a ~0.3% chance
    of a collision somewhere in the run, and a collision here is a FALSE PASS.
    """
    stats_a, stats_b = new_stats(), new_stats()
    digests: dict[str, bytes] = {}
    dupes_a: Counter = Counter()

    def digest(obj) -> bytes:
        return hashlib.blake2b(canon(obj).encode("utf-8"), digest_size=16).digest()

    for rid, _line, obj in scan(path_a, "A", stats_a):
        if rid in digests:
            stats_a["duplicate_ids"] += 1
            dupes_a[rid] += 1
            rid = f"{rid}\x00dup{dupes_a[rid]}"
        digests[rid] = digest(obj)

    differing: set[str] = set()
    only_b: list[str] = []
    matched = 0
    seen_b: set[str] = set()
    dupes_b: Counter = Counter()
    for rid, _line, obj in scan(path_b, "B", stats_b):
        if rid in seen_b:
            stats_b["duplicate_ids"] += 1
            dupes_b[rid] += 1
            rid = f"{rid}\x00dup{dupes_b[rid]}"
        seen_b.add(rid)
        want = digests.pop(rid, None)
        if want is None:
            only_b.append(rid)
            continue
        matched += 1
        if want != digest(obj):
            differing.add(rid)
    del seen_b
    only_a_all = list(digests)
    del digests

    hist: Counter = Counter()
    kinds: Counter = Counter()
    examples: dict[str, list] = {}
    ignored: Counter = Counter()
    differing_ids: list[str] = []

    if differing:
        detail = set(sorted(differing)[:args.max_detail])
        truncated = len(differing) - len(detail)
        lines_a: dict[str, str] = {}
        for rid, line, _obj in scan(path_a, "A", new_stats()):
            if rid in detail:
                lines_a[rid] = line
        for rid, line, _obj in scan(path_b, "B", new_stats()):
            if rid not in detail:
                continue
            line_a = lines_a.get(rid)
            if line_a is None:
                continue
            accumulate(json.loads(line_a, **_PARSE), json.loads(line, **_PARSE),
                       rid, args, hist, kinds, examples, ignored, differing_ids)
        if truncated:
            print(f"  ! detail limited to {len(detail):,} of {len(differing):,} "
                  f"differing records (--max-detail)", file=sys.stderr)

    only_a = sorted(k for k in only_a_all if "\x00" not in k)
    only_b = sorted(k for k in only_b if "\x00" not in k)
    anomalies = sum(s[k] for s in (stats_a, stats_b)
                    for k in ("duplicate_ids", "missing_id", "parse_errors"))
    identical = not (differing or only_a or only_b or anomalies
                     or stats_a["lines"] != stats_b["lines"])

    return {
        "file_a": str(path_a), "file_b": str(path_b),
        "strategy": "digest",
        "stats_a": stats_a, "stats_b": stats_b,
        "common": matched,
        "only_a": only_a, "only_b": only_b,
        "differing_resources": len(differing),
        "differing_ids_sample": sorted(differing)[:args.max_examples],
        "kinds": dict(kinds),
        "histogram": [{"kind": k, "path": p, "count": c}
                      for (k, p), c in hist.most_common()],
        "examples": examples,
        "ignored": dict(ignored),
        "identical": identical,
    }


def accumulate(a, b, rid, args, hist, kinds, examples, ignored, differing_ids):
    """Classify one differing pair into the histogram. Shared by both strategies."""
    out: list = []
    diff(a, b, "$", out)
    kept = False
    for kind, p, detail in out:
        g = generic(p)
        if g in args.ignore or f"{kind}:{g}" in args.ignore:
            ignored[g] += 1
            continue
        kept = True
        hist[(kind, g)] += 1
        kinds[kind] += 1
        ex = examples.setdefault(f"{kind} {g}", [])
        if len(ex) < args.max_examples:
            ex.append({"id": rid, "detail": detail, "path": p})
    if kept:
        differing_ids.append(rid)
    elif not out:
        # a != b but the walk found nothing: bool-vs-number is the only way this
        # can happen (True == Decimal(1) in Python). Worth knowing about.
        hist[("undiffable", "$")] += 1
        kinds["undiffable"] += 1
        differing_ids.append(rid)


def compare_in_memory(path_a: pathlib.Path, path_b: pathlib.Path, args) -> dict:
    index_a, stats_a = index_lines(path_a, "A")
    index_b, stats_b = index_lines(path_b, "B")

    hist: Counter = Counter()
    kinds: Counter = Counter()
    examples: dict[str, list] = {}
    ignored: Counter = Counter()
    differing_ids: list[str] = []

    only_b = []
    matched = 0
    for rid, line_b in index_b.items():
        line_a = index_a.pop(rid, None)
        if line_a is None:
            only_b.append(rid)
            continue
        matched += 1
        if line_a == line_b:            # identical bytes, nothing to normalise
            continue
        a = json.loads(line_a, **_PARSE)
        b = json.loads(line_b, **_PARSE)
        if a == b:                      # key order and/or numeric formatting only
            continue

        accumulate(a, b, rid, args, hist, kinds, examples, ignored, differing_ids)

    # Synthetic keys (no id, duplicate id) never match anything, so they would
    # otherwise be reported as ordinary only-A / only-B ids. They are already
    # counted in stats; drop them here so the id lists stay meaningful.
    only_a = sorted(k for k in index_a if "\x00" not in k)
    only_b = sorted(k for k in only_b if "\x00" not in k)

    anomalies = sum(s[k] for s in (stats_a, stats_b)
                    for k in ("duplicate_ids", "missing_id", "parse_errors"))
    identical = not (hist or only_a or only_b or anomalies
                     or stats_a["lines"] != stats_b["lines"])

    return {
        "file_a": str(path_a), "file_b": str(path_b),
        "strategy": "in-memory",
        "stats_a": stats_a, "stats_b": stats_b,
        "common": matched,
        "only_a": only_a, "only_b": only_b,
        "differing_resources": len(differing_ids),
        "differing_ids_sample": sorted(differing_ids)[:args.max_examples],
        "kinds": dict(kinds),
        "histogram": [{"kind": k, "path": p, "count": c}
                      for (k, p), c in hist.most_common()],
        "examples": examples,
        "ignored": dict(ignored),
        "identical": identical,
    }


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def report(name: str, r: dict, args) -> None:
    sa, sb = r["stats_a"], r["stats_b"]
    print(f"\n== {name} ==")
    print(f"  lines            A={sa['lines']:>9,}  B={sb['lines']:>9,}"
          + ("" if sa["lines"] == sb["lines"]
             else f"   DELTA {sb['lines'] - sa['lines']:+,}"))
    print(f"  ids              common={r['common']:,}"
          f"  only-A={len(r['only_a']):,}  only-B={len(r['only_b']):,}")

    for side, s in (("A", sa), ("B", sb)):
        for key, label in (("duplicate_ids", "DUPLICATE ids"),
                           ("missing_id", "rows with no id"),
                           ("parse_errors", "UNPARSEABLE lines")):
            if s[key]:
                print(f"  ! {side} {label}: {s[key]:,}")

    for label, ids in (("only in A", r["only_a"]), ("only in B", r["only_b"])):
        if ids:
            shown = ", ".join(ids[:args.max_examples])
            more = f" (+{len(ids) - args.max_examples:,} more)" \
                if len(ids) > args.max_examples else ""
            print(f"  ! ids {label}: {shown}{more}")

    if r["ignored"]:
        total = sum(r["ignored"].values())
        print(f"  ~ ignored {total:,} diffs on {len(r['ignored'])} path(s): "
              + ", ".join(sorted(r["ignored"])))

    if not r["histogram"]:
        print("  IDENTICAL" if r["identical"] else "  no field diffs")
        return

    print(f"  resources differing: {r['differing_resources']:,}"
          f" of {r['common']:,}")
    print(f"  {'count':>10}  {'kind':<18}  path")
    shown = r["histogram"][:args.top]
    for row in shown:
        print(f"  {row['count']:>10,}  {row['kind']:<18}  {row['path']}")
    if len(r["histogram"]) > len(shown):
        print(f"  {'':>10}  ... {len(r['histogram']) - len(shown)} more paths"
              f" (--top {len(r['histogram'])} to see all)")

    if args.verbose:
        for row in shown:
            key = f"{row['kind']} {row['path']}"
            for ex in r["examples"].get(key, []):
                print(f"      {row['kind']:<18} {ex['path']}")
                print(f"        id={ex['id']}  {ex['detail']}")


# --------------------------------------------------------------------------

def collect(root: pathlib.Path) -> dict[str, pathlib.Path]:
    if root.is_file():
        return {stem(root): root}
    found: dict[str, pathlib.Path] = {}
    for p in sorted(root.iterdir()):
        if p.is_file() and re.search(r"\.(ndjson|json)(\.gz)?$", p.name):
            found[stem(p)] = p
    return found


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("a", help="reference export (psql baseline)")
    ap.add_argument("b", help="export under test (DuckDB)")
    ap.add_argument("--only", action="append", default=[],
                    help="resource name substring; repeatable")
    ap.add_argument("--exclude", action="append", default=[],
                    help="resource name substring to skip; repeatable. Applied "
                         "after --only. Exists so the two huge resources can be "
                         "split into their own jobs without listing the other 28 "
                         "by hand -- a list that long is one typo away from a "
                         "quietly partial run that still exits 0.")
    ap.add_argument("--ignore", action="append", default=[],
                    help="generic JSON path to ignore, e.g. '$.meta.lastUpdated', "
                         "or 'kind:$.path'. Ignored diffs are counted and reported.")
    ap.add_argument("--max-examples", type=int, default=3)
    ap.add_argument("--digest-threshold-gb", type=float, default=8.0,
                    help="above this input size, compare via 16-byte digests in "
                         "two passes instead of holding both sides in memory "
                         "(default 8 GB). Set very high to force the in-memory "
                         "path, or 0 to force the digest path")
    ap.add_argument("--max-detail", type=int, default=200_000,
                    help="cap on differing records re-read for the field-level "
                         "histogram in the digest path (default 200k)")
    ap.add_argument("--top", type=int, default=25,
                    help="histogram rows per resource (default 25)")
    ap.add_argument("--json", dest="json_out",
                    help="write the full machine-readable report here")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="show example diffs under each histogram row")
    args = ap.parse_args()
    args.ignore = set(args.ignore)

    root_a, root_b = pathlib.Path(args.a), pathlib.Path(args.b)
    for p in (root_a, root_b):
        if not p.exists():
            print(f"no such path: {p}", file=sys.stderr)
            return 2

    files_a, files_b = collect(root_a), collect(root_b)
    if args.only:
        keep = lambda n: any(o.lower() in n.lower() for o in args.only)
        files_a = {k: v for k, v in files_a.items() if keep(k)}
        files_b = {k: v for k, v in files_b.items() if keep(k)}
    if args.exclude:
        drop = lambda n: any(o.lower() in n.lower() for o in args.exclude)
        files_a = {k: v for k, v in files_a.items() if not drop(k)}
        files_b = {k: v for k, v in files_b.items() if not drop(k)}

    # A single explicit file pair is compared regardless of what it is called.
    if root_a.is_file() and root_b.is_file():
        names = [(stem(root_a), root_a, root_b)]
        missing_a = missing_b = []
    else:
        names = [(n, files_a[n], files_b[n])
                 for n in sorted(set(files_a) & set(files_b))]
        missing_b = sorted(set(files_a) - set(files_b))
        missing_a = sorted(set(files_b) - set(files_a))

    if not names and not (missing_a or missing_b):
        print("no comparable ndjson files found", file=sys.stderr)
        return 2

    print(f"A (reference) {root_a}\nB (under test) {root_b}\n"
          f"{len(names)} resource(s) in both")
    for label, miss in (("MISSING from B", missing_b), ("MISSING from A", missing_a)):
        if miss:
            print(f"  ! {label}: {', '.join(miss)}")

    results = {}
    for name, pa, pb in names:
        results[name] = compare(pa, pb, args)
        report(name, results[name], args)

    ok = [n for n, r in results.items() if r["identical"]]
    bad = [n for n, r in results.items() if not r["identical"]]
    print(f"\n{'-' * 60}\n{len(ok)}/{len(results)} resources identical")
    if bad:
        print("differing: " + ", ".join(bad))
    if missing_a or missing_b:
        print("file-level mismatch: see MISSING lines above")

    if args.json_out:
        pathlib.Path(args.json_out).write_text(
            json.dumps({"a": str(root_a), "b": str(root_b),
                        "missing_from_a": missing_a, "missing_from_b": missing_b,
                        "resources": results}, indent=2, default=str))
        print(f"report: {args.json_out}")

    return 0 if not bad and not missing_a and not missing_b else 1


if __name__ == "__main__":
    sys.exit(main())
