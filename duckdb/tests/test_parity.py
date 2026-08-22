#!/usr/bin/env python3
"""Tests for duckdb/parity.py.

A differ that reports "identical" is only useful if you know it would have said
otherwise. Half of these cases are the normalisations parity.py is SUPPOSED to
absorb (key order, numeric formatting, line order) and half are the real port
failure modes it must NOT absorb -- above all DECIMAL(18,3) truncation, which is
a numeric difference and therefore the one thing most at risk of being
normalised away by a differ that compares numbers loosely.

    python3 duckdb/tests/test_parity.py

No Postgres, no DuckDB, no data. Runs anywhere.
"""
import argparse
import gzip
import importlib.util
import json
import pathlib
import sys
import tempfile

# Load by path. `import parity` would work here but `import duckdb.parity` does
# not: the repo's duckdb/ directory is a namespace package that loses to the
# installed PyPI duckdb (HANDOFF.md trap 16). Path loading sidesteps both.
_SPEC = importlib.util.spec_from_file_location(
    "mimic_parity", pathlib.Path(__file__).resolve().parents[1] / "parity.py")
parity = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(parity)

ARGS = argparse.Namespace(max_examples=3, top=25, verbose=False, ignore=set(),
                          digest_threshold_gb=8.0, max_detail=200_000)


def R(rid, **kw):
    """One FHIR-shaped resource line."""
    return dict(resourceType="Observation", id=rid, **kw)


# ---------------------------------------------------------------------------
# (name, A lines, B lines, expected)
#
# expected:
#   identical  bool
#   hist       {(kind, generic_path): count}  -- exact, so an extra diff fails
#   only_a / only_b  counts
# ---------------------------------------------------------------------------
CASES = [
    # -- things that MUST normalise away -----------------------------------
    ("key order",
     [{"id": "a", "resourceType": "Patient", "gender": "male"}],
     [{"resourceType": "Patient", "gender": "male", "id": "a"}],
     dict(identical=True)),

    ("nested key order",
     [R("a", valueQuantity={"value": 1, "unit": "mg", "system": "u"})],
     [R("a", valueQuantity={"system": "u", "unit": "mg", "value": 1})],
     dict(identical=True)),

    # fhir_decimal is CAST AS DOUBLE: Postgres '5.20' comes back 5.2, '7' -> 7.0
    ("numeric formatting: trailing zeros",
     [R("a", valueQuantity={"value": 5.20}), R("b", valueQuantity={"value": 7})],
     [R("a", valueQuantity={"value": 5.2}), R("b", valueQuantity={"value": 7.0})],
     dict(identical=True)),

    ("numeric formatting: exponent",
     [R("a", valueQuantity={"value": 0.000001})],
     [R("a", valueQuantity={"value": 1e-06})],
     dict(identical=True)),

    # preserve_insertion_order = false -- row order is not stable
    ("line order",
     [R("a", x=1), R("b", x=2), R("c", x=3)],
     [R("c", x=3), R("a", x=1), R("b", x=2)],
     dict(identical=True)),

    ("numbers inside arrays",
     [R("a", v=[1, 2.50, 3])],
     [R("a", v=[1.0, 2.5, 3.0])],
     dict(identical=True)),

    # -- things that MUST NOT normalise away --------------------------------
    # Trap 1. This is a NUMERIC difference, so it is the case a loosely
    # comparing differ silently eats. 0.000001 -> 0.000 under DECIMAL(18,3).
    ("DECIMAL(18,3) truncation",
     [R("a", valueQuantity={"value": 0.000001})],
     [R("a", valueQuantity={"value": 0.000})],
     dict(identical=False, hist={("value", "$.valueQuantity.value"): 1})),

    # Trap 2. Session-timezone rendering with a space separator.
    ("timestamp format",
     [R("a", effectiveDateTime="2180-07-23T14:00:00+00:00")],
     [R("a", effectiveDateTime="2180-07-23 14:00:00+00")],
     dict(identical=False, hist={("value", "$.effectiveDateTime"): 1})),

    # strip() placement: a null-valued key surviving on one side
    ("key only in B",
     [R("a")],
     [R("a", status=None)],
     dict(identical=False, hist={("only_b", "$.status"): 1})),

    ("key only in A",
     [R("a", status="final")],
     [R("a")],
     dict(identical=False, hist={("only_a", "$.status"): 1})),

    # HANDOFF §9.1: acuity/pain through the json_each pivot land in valueString,
    # where to_json's 80.0 vs jsonb_each_text's 80 is a genuine string diff.
    ("valueString numeric formatting is a real diff",
     [R("a", valueString="2")],
     [R("a", valueString="2.0")],
     dict(identical=False, hist={("value", "$.valueString"): 1})),

    ("type change number vs string",
     [R("a", v=2)],
     [R("a", v="2")],
     dict(identical=False, hist={("type", "$.v"): 1})),

    # Trap 11: list_denull returning [] would look exactly like this.
    ("array emptied",
     [R("a", location=[{"x": 1}, {"x": 2}])],
     [R("a", location=[])],
     dict(identical=False, hist={("array_length", "$.location"): 1,
                                 ("array_elem_only_a", "$.location[*]"): 2})),

    ("array reordered is its own kind",
     [R("a", name=[{"n": 1}, {"n": 2}, {"n": 3}])],
     [R("a", name=[{"n": 3}, {"n": 1}, {"n": 2}])],
     dict(identical=False, hist={("array_order", "$.name"): 1})),

    # A rotated array must not report every element as changed.
    ("reorder does not cascade",
     [R("a", name=[{"n": i} for i in range(20)])],
     [R("a", name=[{"n": i} for i in [19] + list(range(19))])],
     dict(identical=False, hist={("array_order", "$.name"): 1})),

    ("array element field diff keeps position in examples",
     [R("a", name=[{"given": ["X"]}, {"given": ["Y"]}])],
     [R("a", name=[{"given": ["X"]}, {"given": ["Z"]}])],
     dict(identical=False, hist={("value", "$.name[*].given[*]"): 1})),

    ("ids only on one side",
     [R("a"), R("b")],
     [R("a"), R("c")],
     dict(identical=False, only_a=1, only_b=1)),

    ("duplicate id is not silently deduped",
     [R("a"), R("a")],
     [R("a")],
     dict(identical=False)),

    # The whole point of the histogram: one systematic bug, one row.
    ("systematic diff aggregates to one path",
     [R(str(i), status="final") for i in range(50)],
     [R(str(i), status="FINAL") for i in range(50)],
     dict(identical=False, hist={("value", "$.status"): 50})),

    ("deep path is reported in full",
     [R("a", a={"b": {"c": [{"d": 1}]}})],
     [R("a", a={"b": {"c": [{"d": 2}]}})],
     dict(identical=False, hist={("value", "$.a.b.c[*].d"): 1})),
]


def write(path, rows, gz=False):
    text = "".join(json.dumps(r) + "\n" for r in rows)
    if gz:
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            fh.write(text)
    else:
        path.write_text(text, encoding="utf-8")


def check_result(r, want):
    fails = []
    if r["identical"] != want.get("identical", True):
        fails.append(f"identical={r['identical']} want={want.get('identical')}")
    if "hist" in want:
        got = {(h["kind"], h["path"]): h["count"] for h in r["histogram"]}
        if got != want["hist"]:
            fails.append(f"histogram={got} want={want['hist']}")
    for key in ("only_a", "only_b"):
        if key in want and len(r[key]) != want[key]:
            fails.append(f"{key}={len(r[key])} want={want[key]}")
    return fails


# The fields both strategies must agree on. `strategy` obviously differs, and
# examples/differing_ids_sample are ordered samples that need not match.
AGREE_KEYS = ("identical", "common", "only_a", "only_b", "kinds", "histogram",
              "ignored", "differing_resources", "stats_a", "stats_b")


def run_case(tmp, name, a_rows, b_rows, want, gz=False, args=None):
    """Run every case through BOTH comparison strategies and require agreement.

    The digest path exists only because the in-memory one cannot hold 262 GB. It
    is a second implementation of the same semantics, so the real risk is not
    that it is wrong in isolation but that it drifts from the path the 28/30
    result was produced with. Running the whole table through both, and
    comparing them field by field, is what keeps them honest -- a case that only
    ever runs on one path would not catch drift.
    """
    args = args or ARGS
    pa = tmp / "A.ndjson"
    pb = tmp / ("B.ndjson.gz" if gz else "B.ndjson")
    write(pa, a_rows)
    write(pb, b_rows, gz=gz)

    # threshold above any fixture size -> in-memory; 0 -> digest, unconditionally
    mem = parity.compare(pa, pb, argparse.Namespace(
        **{**vars(args), "digest_threshold_gb": 1e9}))
    dig = parity.compare(pa, pb, argparse.Namespace(
        **{**vars(args), "digest_threshold_gb": 0.0}))

    if mem["strategy"] != "in-memory" or dig["strategy"] != "digest":
        return [f"dispatch wrong: {mem['strategy']} / {dig['strategy']}"]

    fails = check_result(mem, want)
    for key in AGREE_KEYS:
        if mem[key] != dig[key]:
            fails.append(f"strategies disagree on {key}: "
                         f"in-memory={mem[key]!r} digest={dig[key]!r}")
    return fails


def compare_both(pa, pb, args, fails, label):
    """Run both strategies, record any disagreement, return the in-memory result."""
    mem = parity.compare(pa, pb, argparse.Namespace(
        **{**vars(args), "digest_threshold_gb": 1e9}))
    dig = parity.compare(pa, pb, argparse.Namespace(
        **{**vars(args), "digest_threshold_gb": 0.0}))
    for key in AGREE_KEYS:
        if mem[key] != dig[key]:
            fails.append((f"{label}: strategies disagree on {key}",
                          mem[key], dig[key]))
    return mem


def extra_checks(tmp):
    """Cases that do not fit the (A, B, expected) table."""
    fails = []

    # gzip on either side, transparently -- the DuckDB run writes .ndjson.gz
    # while the psql baseline export is uncompressed.
    f = run_case(tmp, "gzip", [R("a", x=1)], [R("a", x=1)], dict(identical=True),
                 gz=True)
    if f:
        fails.append(("gzip B side", f))

    # --ignore suppresses a path, counts it, and does not suppress its neighbours
    args = argparse.Namespace(**{**vars(ARGS), "ignore": {"$.meta.lastUpdated"}})
    pa, pb = tmp / "A.ndjson", tmp / "B.ndjson"
    write(pa, [R("a", meta={"lastUpdated": "1", "v": "1"})])
    write(pb, [R("a", meta={"lastUpdated": "2", "v": "2"})])
    r = compare_both(pa, pb, args, fails, "ignore")
    if r["ignored"] != {"$.meta.lastUpdated": 1}:
        fails.append(("ignore counted", r["ignored"]))
    if [(h["kind"], h["path"]) for h in r["histogram"]] != [("value", "$.meta.v")]:
        fails.append(("ignore is not a blanket", r["histogram"]))

    # An unparseable line must be loud, not skipped into a passing result.
    (tmp / "A.ndjson").write_text('{"id":"a"}\nnot json\n')
    (tmp / "B.ndjson").write_text('{"id":"a"}\n')
    r = compare_both(tmp / "A.ndjson", tmp / "B.ndjson", ARGS, fails, "parse error")
    if r["identical"] or r["stats_a"]["parse_errors"] != 1:
        fails.append(("parse error surfaced", r["stats_a"]))

    # A resource with no id cannot be matched by id; it must be counted, not
    # quietly paired with whatever happens to be on the same line number.
    write(tmp / "A.ndjson", [{"resourceType": "Patient"}])
    write(tmp / "B.ndjson", [{"resourceType": "Patient"}])
    r = compare_both(tmp / "A.ndjson", tmp / "B.ndjson", ARGS, fails, "missing id")
    if r["identical"] or r["stats_a"]["missing_id"] != 1 or r["only_a"] or r["only_b"]:
        fails.append(("missing id surfaced", r["stats_a"], r["only_a"], r["only_b"]))

    # Exact numeric equality must hold for values a float round-trip would blur.
    write(tmp / "A.ndjson", [R("a", v=0.1 + 0.2)])
    write(tmp / "B.ndjson", [R("a", v=0.3)])
    r = compare_both(tmp / "A.ndjson", tmp / "B.ndjson", ARGS, fails, "float")
    if r["identical"]:
        fails.append(("0.30000000000000004 != 0.3", r["histogram"]))

    return fails


def main():
    failed = []
    with tempfile.TemporaryDirectory() as td:
        tmp = pathlib.Path(td)
        for name, a, b, want in CASES:
            fails = run_case(tmp, name, a, b, want)
            print(f"{'ok  ' if not fails else 'FAIL'}  {name}")
            for f in fails:
                print(f"        {f}")
            if fails:
                failed.append(name)

        for name, *detail in extra_checks(tmp):
            print(f"FAIL  {name}")
            print(f"        {detail}")
            failed.append(name)
        if not failed:
            print("ok    gzip / --ignore / parse errors / missing id / float exactness")

    total = len(CASES) + 5
    print(f"\n{total - len(failed)}/{total} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
