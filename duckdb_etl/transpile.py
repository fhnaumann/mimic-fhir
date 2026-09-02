#!/usr/bin/env python3
"""Transpile the MIMIC-IV-on-FHIR mapping SQL from Postgres dialect to DuckDB.

    sql/**.sql  ->  build/duckdb/**.sql  +  build/duckdb/manifest.json

`sql/*.sql` stays the single source of truth, in Postgres dialect, so a mapping fix
lands once and both backends get it. Everything backend-specific lives either here
(as a rewrite rule) or in duckdb_etl/runtime/10_macros.sql (as a macro). Nowhere else.

Design notes that are not obvious from the code:

  * sqlglot's Expression.transform() PRUNES replaced subtrees -- it stops descending
    into any node the callback returns fresh, so the obvious implementation silently
    drops every nested rewrite. Renames are therefore done IN PLACE (`node.set(...)`)
    and structural replacements run as a separate deepest-first pass. See rewrite().

  * We do not trust sqlglot's built-in postgres->duckdb translation for anything we
    care about. Its ARRAY_REMOVE rule is actively wrong (rule 8) and its regexp and
    json handling is incomplete (rules 8c, 1-3). Rules run before .sql(dialect=...)
    so our node is what gets rendered.

  * Output is TEMP VIEWs, not COPY. driver.py owns the I/O policy, because the
    compression choice is a benchmark variable and baking it into generated SQL
    would make the gzip/zstd runs differ by more than one setting.

Usage:
    python duckdb_etl/transpile.py                    # transpile + guard rails
    python duckdb_etl/transpile.py --explain          # ... and EXPLAIN every statement
    python duckdb_etl/transpile.py --only fhir_patient.sql
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from collections import Counter

try:
    import sqlglot
    from sqlglot import exp
except ImportError:  # pragma: no cover
    sys.exit("sqlglot is required: pip install sqlglot==30.17.0")

REPO = pathlib.Path(__file__).resolve().parent.parent
SRC = REPO / "sql"
OUT = REPO / "build" / "duckdb"

READ_DIALECT = "postgres"
WRITE_DIALECT = "duckdb"

BANNER = "-- generated from {src} by duckdb_etl/transpile.py -- do not edit\n"


# --------------------------------------------------------------------------
# Files the port deliberately does not translate.
#
# Whitelisted rather than skipped-on-error: a NEW parse failure must still abort,
# and it would not if we swallowed every failure. Each entry carries its reason.
# --------------------------------------------------------------------------
DROPPED = {
    "create_fhir_jsons.sql":
        "psql meta-commands only; parsed separately for the resource->filename map",
    "create_table_clusters.sql":
        "CLUSTER has no DuckDB equivalent and no purpose without heap tables",
    "create_mimic_index.sql":
        "indexes on the source tables; DuckDB scans Parquet, no indexes to build",
    "create_single_fhir_resource.sql":
        "dev helper, not reachable from create_fhir_tables.sql",
    "codesystem/count_records_fhir_trm.sql":
        "reporting helper using xpath/query_to_xml; not reachable from any include",
    "validate_fhir_tables.sql":
        "row-count gate against mimic_fhir tables; driver.py runs it against the views",
    "validate_fhir_tables_demo.sql":
        "as above, demo variant",
    # The 5 plpgsql functions. sqlglot parses these as a Create with an opaque $$
    # body and will not port them, as expected -- they are hand-written as DuckDB
    # macros in duckdb_etl/runtime/20_functions.sql and pinned by tests/test_functions.py
    # against recorded Postgres output.
    "fn/fn_patient_extension.sql": "hand-ported to runtime/20_functions.sql",
    "fn/fn_med_statement.sql": "hand-ported to runtime/20_functions.sql",
    "fn/fn_build_medication_identifier.sql": "hand-ported to runtime/20_functions.sql",
    "fn/fn_prescriptions_medication_code.sql": "hand-ported to runtime/20_functions.sql",
    "fn/fn_create_table_patient_dependent.sql":
        "creates a table and its indexes; the port has neither (rule 10)",
}

# Pure include drivers. We read their \i order but emit no SQL for them: the schema
# DDL and SET they carry are owned by driver.py and runtime/00_settings.sql.
INCLUDE_DRIVERS = {
    "create_fhir_tables.sql",
    "create_fhir_terminology.sql",
    "create_fhir_medication.sql",
}

# Schemas that stay REAL TABLES. fhir_trm.* has an external consumer
# (py_mimic_fhir/terminology.py reads it over DBAPI) and fhir_etl.* is joined
# against; both are small. Only mimic_fhir.* becomes files.
TABLE_SCHEMAS = {"fhir_etl", "fhir_trm"}
VIEW_SCHEMA = "mimic_fhir"

META_LINE = re.compile(r"^\s*\\")
INCLUDE = re.compile(r"^\s*\\i\s+(\S+)")


# --------------------------------------------------------------------------
# Rule 5 diagnostic support.
#
# fhir_instant() returns VARCHAR. In a JSON position that is exactly right; in a
# comparison it is a different ordering unless the format happens to sort
# lexicographically. '%Y-%m-%dT%H:%M:%S+00:00' is fixed-width and zero-padded, so
# it does -- for 4-digit years. The audit found exactly one comparison site
# (ph_STARTTIME <= ph_STOPTIME in fhir_medication_request.sql); this list records
# the sites we have reasoned about so a NEW one shows up as an unreviewed warning.
# --------------------------------------------------------------------------
REVIEWED_INSTANT_COMPARISONS = {
    ("fhir_medication_request.sql", "ph_STARTTIME <= ph_STOPTIME"),
}


class Stats(Counter):
    def bump(self, rule: str, n: int = 1):
        self[rule] += n


# ==========================================================================
# Preprocessing
# ==========================================================================

def strip_meta(text: str) -> str:
    """Drop psql backslash meta-commands so sqlglot sees plain SQL."""
    return "\n".join("" if META_LINE.match(l) else l for l in text.splitlines())


def parse_file(path: pathlib.Path):
    """Parse one file.

    22 of the 109 files do not end in a semicolon and rely on psql flushing its
    query buffer at EOF, so each file is treated as an implicit statement
    terminator. That is automatic here because we parse per file and never
    concatenate before parsing.
    """
    text = strip_meta(path.read_text())
    return [t for t in sqlglot.parse(text, read=READ_DIALECT) if t is not None]


def read_include_order(root: str) -> list[str]:
    """Follow \\i directives depth-first, the order psql would execute them in."""
    order: list[str] = []
    seen: set[str] = set()

    def walk(rel: str):
        if rel in seen:
            return
        seen.add(rel)
        p = SRC / rel
        if not p.exists():
            return
        is_driver = rel in INCLUDE_DRIVERS
        if not is_driver:
            order.append(rel)
        for line in p.read_text().splitlines():
            m = INCLUDE.match(line)
            if m:
                walk(m.group(1))

    walk(root)
    return order


def read_output_map() -> dict[str, str]:
    """Extract mimic_fhir table -> ndjson filename from create_fhir_jsons.sql.

    Parsed, not hardcoded: the mapping is not mechanical
    (observation_vital_signs -> MimicObservationVitalSignsED.ndjson), and this is
    the only place it is written down. One source of truth.
    """
    text = (SRC / "create_fhir_jsons.sql").read_text()
    out: dict[str, str] = {}
    pending: str | None = None
    for line in text.splitlines():
        m = re.match(r"\s*\\set\s+outputfile\s+:outputdir/(\S+\.ndjson)", line)
        if m:
            pending = m.group(1)
            continue
        m = re.search(r"\\\\copy\s+mimic_fhir\.(\w+)\(fhir\)", line)
        if m and pending:
            out[m.group(1)] = pending
            pending = None
    return out


# ==========================================================================
# Node constructors
# ==========================================================================

def call(name: str, *args) -> exp.Anonymous:
    return exp.Anonymous(this=name, expressions=list(args))


def is_call(node, *names) -> bool:
    return isinstance(node, exp.Anonymous) and node.name.lower() in names


def depth(node) -> int:
    d, cur = 0, node.parent
    while cur is not None:
        d += 1
        cur = cur.parent
    return d


def deepest_first(nodes):
    """Replacement order that keeps parents valid while children are swapped."""
    return sorted(nodes, key=depth, reverse=True)


# ==========================================================================
# The rewrite rules
# ==========================================================================

def rewrite(tree, stats: Stats, diagnostics: list, filename: str):
    """Apply every rule to one statement, in dependency order."""

    # ---- Pass A: renames, IN PLACE ---------------------------------------
    # In place because transform()/replace() on a rename would prune the subtree
    # and silently drop every nested rewrite beneath it.
    for node in tree.walk():
        if not isinstance(node, exp.Anonymous):
            continue
        name = node.name.lower()

        # Rule 1 / 2: jsonb builders -> DuckDB json builders
        if name in ("jsonb_build_object", "json_build_object"):
            node.set("this", "json_object")
            stats.bump("1 json_object")
        elif name in ("jsonb_build_array", "json_build_array"):
            node.set("this", "json_array")
            stats.bump("2 json_array")

        # Rule 4: uuid-ossp -> runtime macros
        elif name == "uuid_generate_v5":
            node.set("this", "uuid_v5")
            stats.bump("4 uuid_v5")
        elif name == "uuid_ns_oid":
            node.set("this", "ns_oid")
            stats.bump("4 ns_oid")

    # ---- Pass B: structural replacements, deepest-first -------------------
    # Rule 6: fhir_etl.fn_*(...) -> fn_*(...).
    # sqlglot does NOT fold the schema into the Anonymous name -- it builds
    # Dot(Identifier(fhir_etl), Anonymous(fn_x)) -- so this is a structural
    # replacement, not a rename. DuckDB macros live in the default schema.
    for node in deepest_first([n for n in tree.walk() if isinstance(n, exp.Dot)]):
        inner = node.expression
        if (isinstance(node.this, exp.Identifier)
                and node.this.name.lower() in TABLE_SCHEMAS
                and isinstance(inner, exp.Anonymous)
                and inner.name.lower().startswith("fn_")):
            node.replace(inner.copy())
            stats.bump("6 unqualify fn_")

    # Rule 3: jsonb_strip_nulls(x) -> x. Redundant once rule 1b wraps every
    # json_object in strip(), and leaving it would be a call to a function that
    # does not exist in DuckDB.
    for node in deepest_first([n for n in tree.walk()
                               if is_call(n, "jsonb_strip_nulls", "json_strip_nulls")]):
        node.replace(node.expressions[0].copy())
        stats.bump("3 drop strip_nulls")

    # Rule 7: jsonb_agg(x ORDER BY ...) -> to_json(list(x ORDER BY ...)).
    # json_group_array is a MACRO in DuckDB, not an aggregate, so ORDER BY inside
    # it is a hard error -- and the ordering is load-bearing in medication_mix
    # (MAIN before BASE before ADDITIVE). to_json(list(...)) keeps it and emits
    # compact JSON; list(...)::JSON would emit spaces.
    # Two node shapes, not one: sqlglot parses `jsonb_agg` as a generic Anonymous
    # but `json_agg` as a typed exp.JSONArrayAgg, and only the Anonymous form is
    # visible to is_call(). Matching just the first leaves the single json_agg site
    # (fhir_observation_micro_test.sql:61) to sqlglot's own renderer, which emits
    # JSON_ARRAYAGG -- a function DuckDB does not have. FORBIDDEN now scans for it.
    agg_nodes = [n for n in tree.walk()
                 if is_call(n, "jsonb_agg", "json_agg") or isinstance(n, exp.JSONArrayAgg)]
    for node in deepest_first(agg_nodes):
        inner = node.expressions[0] if node.expressions else node.this
        node.replace(call("to_json", call("list", inner.copy())))
        stats.bump("7 jsonb_agg")

    # Rule 8: ARRAY_REMOVE(a, NULL) -> list_denull(a).
    # sqlglot parses this into exp.ArrayRemove and its own duckdb rule renders
    # list_filter(a, _u -> _u <> NULL), which keeps NOTHING (x <> NULL is never
    # true). That would empty Encounter.location on every ICU stay, silently.
    for node in deepest_first([n for n in tree.walk() if isinstance(n, exp.ArrayRemove)]):
        arr = node.this
        removed = node.expression
        if not isinstance(removed, exp.Null):
            diagnostics.append(
                f"{filename}: ARRAY_REMOVE removing a non-NULL value "
                f"({removed.sql()}) -- rule 8 only handles NULL removal")
            continue
        node.replace(call("list_denull", arr.copy()))
        stats.bump("8 list_denull")

    # Rule 8b: the row -> key/value pivot.
    #   jsonb_each_text(to_jsonb(t)) AS x("key", value)
    #     -> json_each(to_json(t)) AS x
    # and the x.* that consumes it becomes an explicit two-column projection.
    # DuckDB's json_each returns EIGHT columns (key, value, type, atom, id,
    # parent, fullkey, path), so leaving x.* alone would widen the CTE from 2
    # columns to 8. The value must be read with json_extract_string(...,'$'):
    # x.value and x.atom both stay JSON-typed and keep their quoting.
    #
    # Native UNPIVOT is NOT an option here -- it drops columns whose value is
    # NULL, where jsonb_each_text emits them, which would change row counts in
    # the two largest ED resources.
    for tbl in [n for n in tree.walk() if isinstance(n, exp.Table)]:
        fn = tbl.this
        if not is_call(fn, "jsonb_each_text", "json_each_text"):
            continue
        arg = fn.expressions[0]
        if not is_call(arg, "to_jsonb", "to_json"):
            diagnostics.append(f"{filename}: jsonb_each_text over an unexpected argument "
                               f"{arg.sql()} -- rule 8b expects to_jsonb(<alias>)")
            continue

        alias_node = tbl.args.get("alias")
        tf_alias = alias_node.name if alias_node else None
        cols = [c.name for c in (alias_node.args.get("columns") or [])] if alias_node else []
        if [c.lower() for c in cols] != ["key", "value"]:
            diagnostics.append(f"{filename}: jsonb_each_text aliased as {cols} -- "
                               f"rule 8b expects (key, value)")
            continue

        tbl.set("this", call("json_each", call("to_json", arg.expressions[0].copy())))
        alias_node.set("columns", [])  # json_each names its own columns

        # Expand the matching x.* into the two columns Postgres would have given.
        select = tbl.parent_select
        if select is None:
            diagnostics.append(f"{filename}: rule 8b found no enclosing SELECT")
            continue
        expanded = False
        for i, proj in enumerate(list(select.expressions)):
            if (isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star)
                    and proj.table == tf_alias):
                key_col = exp.alias_(exp.column("key", tf_alias), "key", quoted=True)
                val_col = exp.alias_(
                    call("json_extract_string",
                         exp.column("value", tf_alias),
                         exp.Literal.string("$")),
                    "value")
                select.set("expressions",
                           select.expressions[:i] + [key_col, val_col]
                           + select.expressions[i + 1:])
                expanded = True
                break
        if not expanded:
            diagnostics.append(f"{filename}: rule 8b rewrote json_each but found no "
                               f"{tf_alias}.* to expand -- check the projection by hand")
        stats.bump("8b json_each pivot")

    # Rule 8c: (regexp_match(s, p [,flags]))[n] -> nullif(regexp_extract(s,p,n[,flags]),'')
    #
    # Postgres regexp_match returns NULL when the pattern does not match; DuckDB's
    # regexp_extract returns the EMPTY STRING. The two sites feed a numeric cast, so
    # without the nullif a non-matching fill_quantity would hit CAST('' AS DOUBLE)
    # and error out mid-build instead of producing NULL.
    for br in deepest_first([n for n in tree.walk() if isinstance(n, exp.Bracket)]):
        fn = br.this
        while isinstance(fn, exp.Paren):  # the source writes (regexp_match(...))[n]
            fn = fn.this
        if not is_call(fn, "regexp_match", "regexp_matches"):
            continue
        idx = br.expressions[0]
        if not isinstance(idx, exp.Literal) or idx.is_string:
            diagnostics.append(f"{filename}: non-literal subscript on regexp_match "
                               f"({br.sql()}) -- rule 8c needs a constant group number")
            continue
        # sqlglot normalises Postgres's 1-based subscript to 0-based on the way in:
        # source [1] is stored as Literal(0). DuckDB's regexp_extract numbers capture
        # groups from 1, the same as Postgres, so convert back rather than passing the
        # stored value straight through -- that would shift every group by one.
        group = int(idx.this) + 1
        args = [fn.expressions[0].copy(), fn.expressions[1].copy(),
                exp.Literal.number(group)]
        if len(fn.expressions) > 2:  # the 'i' case-insensitivity flag
            args.append(fn.expressions[2].copy())
        br.replace(call("nullif", call("regexp_extract", *args), exp.Literal.string("")))
        stats.bump("8c regexp_extract")

    # Rule 5 / 5b: the casts. Deepest-first so a cast inside a cast still lands.
    for cast in deepest_first([n for n in tree.walk() if isinstance(n, exp.Cast)]):
        target = cast.to.sql().upper()

        # Rule 5: TIMESTAMPTZ -> fhir_instant (VARCHAR, Postgres jsonb format).
        if target in ("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"):
            new = call("fhir_instant", cast.this.copy())
            _diagnose_instant_context(cast, filename, diagnostics)
            cast.replace(new)
            stats.bump("5 fhir_instant")

        # Rule 5b: NUMERIC/DECIMAL -> fhir_decimal (DOUBLE).
        # A bare DECIMAL in DuckDB is DECIMAL(18,3): '0.000001' becomes 0.000
        # with no error. Every site here is text -> numeric on a lab or med value.
        elif target in ("NUMERIC", "DECIMAL") and not cast.to.expressions:
            cast.replace(call("fhir_decimal", cast.this.copy()))
            stats.bump("5b fhir_decimal")

    # ---- Pass C: rule 1b, wrap every json_object in strip() ---------------
    # Postgres jsonb_strip_nulls removes null-valued keys from every object at
    # any depth, including objects inside arrays. DuckDB's json_merge_patch
    # recurses objects but NOT arrays, so one strip() at the top is not enough.
    # Stripping every object individually removes exactly the same set of keys
    # and is far simpler than computing minimal placement -- and it measured
    # marginally faster, so there is no reason to be clever.
    #
    # Only ever wrap an actual json_object node. Wrapping a CASE or a NULL would
    # turn a NULL branch into {} and stop the caller stripping it away.
    for node in deepest_first([n for n in tree.walk() if is_call(n, "json_object")]):
        if is_call(node.parent, "strip"):
            continue
        node.replace(call("strip", node.copy()))
        stats.bump("1b strip")

    return tree


def _diagnose_instant_context(cast, filename: str, diagnostics: list):
    """Warn when a fhir_instant() result flows somewhere VARCHAR semantics differ.

    A select-list alias is fine on its own; what matters is whether that alias is
    later compared. driver.py cannot see that, so we report it and check the site
    against REVIEWED_INSTANT_COMPARISONS.
    """
    cur, hops = cast.parent, 0
    while cur is not None and hops < 8:
        if isinstance(cur, (exp.Binary, exp.Between, exp.Ordered, exp.Interval)) \
                and not isinstance(cur, exp.DPipe):
            snippet = cur.sql()
            if (filename, snippet) not in REVIEWED_INSTANT_COMPARISONS:
                diagnostics.append(
                    f"{filename}: fhir_instant() lands in a comparison context "
                    f"({snippet[:70]}) -- VARCHAR ordering, review it")
            return
        if is_call(cur, "json_object", "json_array", "jsonb_build_object", "jsonb_build_array"):
            return  # JSON position, which is what fhir_instant is for
        cur = cur.parent
        hops += 1


def check_alias_comparisons(trees, filename: str, diagnostics: list):
    """Second half of the rule 5 diagnostic.

    Rule 5 usually fires on `CAST(col AS TIMESTAMPTZ) AS some_alias` in a CTE
    select list, and the risk is downstream: some_alias compared in a later CTE.
    Collect the aliases rule 5 produced, then look for them in comparisons.
    """
    aliases = set()
    for tree in trees:
        for node in tree.walk():
            if isinstance(node, exp.Alias) and any(
                    is_call(n, "fhir_instant") for n in node.this.walk()):
                aliases.add(node.alias.lower())
    if not aliases:
        return
    for tree in trees:
        for node in tree.walk():
            if not isinstance(node, (exp.Binary, exp.Between, exp.Ordered)):
                continue
            if isinstance(node, (exp.DPipe, exp.Is, exp.And, exp.Or, exp.EQ, exp.NEQ)):
                continue  # equality and NULL tests are string-safe
            used = {c.name.lower() for c in node.find_all(exp.Column)} & aliases
            if not used:
                continue
            snippet = node.sql()
            if (filename, snippet) in REVIEWED_INSTANT_COMPARISONS:
                continue
            diagnostics.append(
                f"{filename}: timestamp alias {sorted(used)} compared as VARCHAR "
                f"({snippet[:70]}) -- review ordering")


# ==========================================================================
# Statement-level policy
# ==========================================================================

def qualified_name(table: exp.Table) -> tuple[str, str]:
    return (table.db or "").lower(), (table.name or "").lower()


def insert_body(insert: exp.Insert) -> exp.Expression:
    """The SELECT feeding an INSERT, with its CTEs reattached.

    `WITH tb AS (...) INSERT INTO t SELECT ... FROM tb` parses with the CTEs on
    the INSERT node (args['with_']), NOT on the SELECT. Rendering insert.expression
    alone therefore drops every CTE and produces a view referencing tables that do
    not exist -- silently, because the view body is not resolved until first use.
    Nearly every mapping file has this shape.
    """
    body = insert.expression.copy()
    cte = insert.args.get("with_")
    if cte is not None:
        body.set("with_", cte.copy())
    return body


def emit_statement(tree, stats: Stats, views: dict, filename: str):
    """Decide what a top-level statement becomes. Returns SQL text or None to drop."""

    # SET is owned by runtime/00_settings.sql, which is sized from the SLURM
    # allocation. A SET TimeZone here would be harmless but misleading.
    if isinstance(tree, exp.Set):
        stats.bump("10 drop SET")
        return None

    # CREATE EXTENSION "uuid-ossp" -- replaced by the uuid_v5 macro.
    if isinstance(tree, exp.Command):
        stats.bump("10 drop command")
        return None

    if isinstance(tree, exp.Semicolon):
        return None

    # SELECT fhir_etl.fn_create_table_patient_dependent('x') -- creates a table and
    # its indexes. No tables in this port, so the whole call goes.
    if isinstance(tree, exp.Select):
        if any(is_call(n, "fn_create_table_patient_dependent") for n in tree.walk()):
            stats.bump("10 drop fn_create_table")
            return None

    if isinstance(tree, exp.Drop):
        kind = (tree.args.get("kind") or "").upper()
        target = tree.this
        if kind == "SCHEMA":
            name = (target.name if isinstance(target, exp.Table) else str(target)).lower()
            if name == VIEW_SCHEMA:
                stats.bump("10 drop mimic_fhir schema DDL")
                return None
            return tree.sql(dialect=WRITE_DIALECT, pretty=True)
        if isinstance(target, exp.Table):
            schema, _ = qualified_name(target)
            if schema == VIEW_SCHEMA:
                stats.bump("10 drop mimic_fhir DROP TABLE")
                return None
        return tree.sql(dialect=WRITE_DIALECT, pretty=True)

    if isinstance(tree, exp.Create):
        kind = (tree.args.get("kind") or "").upper()
        target = tree.this
        if kind == "SCHEMA":
            name = (target.name if isinstance(target, exp.Table) else str(target)).lower()
            if name == VIEW_SCHEMA:
                stats.bump("10 drop mimic_fhir schema DDL")
                return None
            return tree.sql(dialect=WRITE_DIALECT, pretty=True)
        # CREATE TABLE
        tbl = target.this if isinstance(target, exp.Schema) else target
        if isinstance(tbl, exp.Table):
            schema, _ = qualified_name(tbl)
            if schema == VIEW_SCHEMA:
                stats.bump("10 drop mimic_fhir CREATE TABLE")
                return None
        return tree.sql(dialect=WRITE_DIALECT, pretty=True)

    if isinstance(tree, exp.Insert):
        target = tree.this
        tbl = target.this if isinstance(target, exp.Schema) else target
        if isinstance(tbl, exp.Table):
            schema, name = qualified_name(tbl)
            if schema == VIEW_SCHEMA:
                # Rule 9 (revised): emit a view, not a COPY.
                #
                # driver.py then chooses COPY (SELECT fhir::VARCHAR FROM v_X) TO ...
                # for the ndjson export, or CREATE TABLE mimic_fhir.X AS SELECT *
                # FROM v_X for the subject-subset the HAPI validation shim reads
                # over DBAPI. Baking COPY in here would hardcode the compression
                # choice into transpiler output, which is exactly the variable the
                # gzip-now/zstd-later benchmark is trying to isolate.
                #
                # Emission is DEFERRED to the end of the file, because four tables
                # are populated by two INSERTs each and a view is replaced where a
                # table would have been appended to. See finish_views().
                views.setdefault(name, []).append(
                    insert_body(tree).sql(dialect=WRITE_DIALECT, pretty=True))
                stats.bump("9 view part")
                return None
        return tree.sql(dialect=WRITE_DIALECT, pretty=True)

    return tree.sql(dialect=WRITE_DIALECT, pretty=True)


# ==========================================================================
# Guard rails
# ==========================================================================

# A silent no-op rule is the failure mode these catch: the build succeeds, the
# output is wrong, and nothing says so until parity fails a week later.
FORBIDDEN = [
    (re.compile(r"\bjsonb\w*", re.I), "jsonb (rules 1/2/3/8b missed a site)"),
    (re.compile(r"\bfhir_etl\.fn_", re.I), "schema-qualified fn_ (rule 6)"),
    (re.compile(r"\buuid_generate_v5\b", re.I), "uuid_generate_v5 (rule 4)"),
    (re.compile(r"\buuid_ns_oid\b", re.I), "uuid_ns_oid (rule 4)"),
    (re.compile(r"\bTIMESTAMPTZ\b", re.I), "TIMESTAMPTZ (rule 5)"),
    (re.compile(r"\bCLUSTER\b", re.I), "CLUSTER (rule 10)"),
    (re.compile(r"\bARRAY_REMOVE\b", re.I), "ARRAY_REMOVE (rule 8)"),
    (re.compile(r"<>\s*NULL"), "sqlglot's broken ARRAY_REMOVE lambda (rule 8)"),
    (re.compile(r"\bregexp_match\b", re.I), "regexp_match (rule 8c)"),
    # sqlglot's rendering of json_agg. Does not contain the string "jsonb", so the
    # pattern above misses it, and DuckDB has no such function -- it failed at
    # CREATE VIEW time on real data rather than at transpile time.
    (re.compile(r"\bjson_arrayagg\b", re.I), "JSON_ARRAYAGG (rule 7)"),
    # json_group_array is a MACRO in DuckDB, not an aggregate: ORDER BY inside it
    # is a hard error and the ordering is load-bearing in medication_mix.
    (re.compile(r"\bjson_group_array\b", re.I), "json_group_array (rule 7)"),
    (re.compile(r"_u\s*->"), "deprecated arrow lambda"),
    (re.compile(r"\bfn_create_table_patient_dependent\b", re.I), "table creation (rule 10)"),
    (re.compile(r"\bmimic_fhir\.", re.I), "mimic_fhir table reference (rule 9)"),
]


def scan_forbidden(sql_text: str, filename: str) -> list[str]:
    problems = []
    # Comments legitimately mention these words; strip them before scanning.
    body = re.sub(r"--[^\n]*", "", sql_text)
    for pattern, why in FORBIDDEN:
        if pattern.search(body):
            problems.append(f"{filename}: forbidden {why}")
    return problems


def _duckdb_module():
    """Import the PyPI duckdb package, or explain why it is unavailable."""
    try:
        import duckdb
    except ImportError:
        return None, "duckdb python module not installed (pip install duckdb)"
    return duckdb, None


VIEW_PREFIX = re.compile(r"^\s*CREATE OR REPLACE TEMP VIEW\s+\w+\s+AS\s+", re.I | re.S)


def syntax_check(statements: list[tuple[str, str]]) -> list[str]:
    """Parse every generated statement with DuckDB's own parser, without binding.

    json_serialize_sql() parses but does not bind, so this validates syntax against
    the real grammar with no tables, no schemas and no data -- it runs anywhere,
    including the login node before staging exists. That makes it a DEFAULT guard
    rail: a rule that emits malformed SQL is caught in seconds rather than an hour
    into a build.

    It only accepts SELECT statements, so a generated view is checked by stripping
    the CREATE VIEW preamble and serialising the body -- which is where the rewrite
    rules actually operate, and so where a typo would be. Short DDL (CREATE TABLE,
    DROP) is checked with EXPLAIN instead; it references no tables, so it binds.

    Binding errors in view bodies (unknown column, wrong macro arity) are NOT caught
    here. That is what --explain is for, once stage.py has produced real tables.
    """
    duckdb, err = _duckdb_module()
    if duckdb is None:
        return [f"syntax check skipped: {err}"]
    con = duckdb.connect()
    problems = []
    for filename, stmt in statements:
        body = VIEW_PREFIX.sub("", stmt)
        if body != stmt:  # a view: parse-only check on the body
            payload = json.loads(
                con.execute("SELECT json_serialize_sql(?)", [body]).fetchone()[0])
            if payload.get("error"):
                problems.append(
                    f"{filename}: {payload.get('error_subtype', 'PARSE')} at position "
                    f"{payload.get('position')}: {payload.get('error_message')}")
            continue
        try:
            con.execute("EXPLAIN " + stmt)
        except Exception as e:
            msg = str(e).splitlines()[0]
            if msg.startswith(("Parser Error", "Syntax Error")):
                problems.append(f"{filename}: {msg[:160]}")
    return problems


def explain_all(statements: list[tuple[str, str]], setup_sql: str) -> list[str]:
    """EXPLAIN every generated statement against a DuckDB with the schemas attached.

    Unlike syntax_check this BINDS, so it catches unknown columns and unknown
    functions -- but it needs the source tables to exist. Catalog Errors for tables
    stage.py has not created yet are reported separately so they do not drown the
    real findings.
    """
    duckdb, err = _duckdb_module()
    if duckdb is None:
        return [f"--explain skipped: {err}"]
    con = duckdb.connect()
    con.execute(setup_sql)
    problems, missing = [], set()

    # Pass 1: EXECUTE all DDL. fhir_etl.* and fhir_trm.* are built by the generated
    # SQL itself, so without creating them every statement that joins them would
    # report a spurious missing table and the check would be all noise. This runs as
    # a separate pass because statements arrive in filename order, not include order,
    # so a codesystem INSERT can precede the fhir_etl DDL it depends on.
    is_ddl = re.compile(r"\s*(CREATE|DROP)\s+(TABLE|SCHEMA)", re.I)
    for filename, stmt in statements:
        if not is_ddl.match(stmt):
            continue
        try:
            con.execute(stmt)
        except Exception as e:
            problems.append(f"{filename}: DDL failed: {str(e).splitlines()[0][:160]}")

    # Pass 2: bind everything else.
    for filename, stmt in statements:
        if is_ddl.match(stmt):
            continue
        try:
            con.execute("EXPLAIN " + stmt)
        except Exception as e:
            msg = str(e).splitlines()[0]
            m = re.search(r'(?:Table|Schema) with name "?([\w.]+)"? does not exist', msg)
            if m and msg.startswith("Catalog Error"):
                missing.add(m.group(1))
                continue
            problems.append(f"{filename}: {msg[:160]}")
    if missing:
        print(f"\n--explain: {len(missing)} source objects not staged; the statements "
              f"using them could not be bound. Re-run after stage.py to get full "
              f"coverage. Not staged: {', '.join(sorted(missing)[:6])}"
              f"{' ...' if len(missing) > 6 else ''}")
    return problems


# ==========================================================================
# Main
# ==========================================================================

def finish_views(file_views: dict[str, list[str]], views: dict[str, str],
                 stats: Stats) -> list[str]:
    """Turn the deferred INSERT bodies for one file into view definitions.

    Postgres APPENDS to a table on a second INSERT; CREATE OR REPLACE VIEW
    REPLACES. Four resources are built by two INSERTs each -- procedure_ed,
    medication_request, and both large ED observations (observation_ed,
    observation_vital_signs) -- so emitting one view per INSERT would silently
    drop the first half of each. Multi-part tables become numbered part views
    UNION ALL'd together.

    Part views rather than a parenthesised inline union because each INSERT
    carries its own WITH clause, and `WITH ... SELECT ... UNION ALL WITH ...`
    is not valid SQL. Naming the parts also keeps the generated file readable.
    """
    out = []
    for table, parts in file_views.items():
        view = f"v_{table}"
        views[table] = view
        if len(parts) == 1:
            out.append(f"CREATE OR REPLACE TEMP VIEW {view} AS\n{parts[0]}")
        else:
            names = []
            for i, body in enumerate(parts, 1):
                part = f"{view}__part{i}"
                names.append(part)
                out.append(f"CREATE OR REPLACE TEMP VIEW {part} AS\n{body}")
            union = "\nUNION ALL\n".join(f"SELECT * FROM {n}" for n in names)
            out.append(
                f"-- {table} is populated by {len(parts)} INSERTs in the source; "
                f"a view replaces where a table appended.\n"
                f"CREATE OR REPLACE TEMP VIEW {view} AS\n{union}")
            stats.bump("9 multi-part view")
    return out


def transpile_file(path: pathlib.Path, stats: Stats, views: dict,
                   diagnostics: list) -> tuple[str, list[str]]:
    """Returns (file text, list of individual statements).

    The statements are returned separately rather than re-split from the text:
    splitting generated SQL on ';' is exactly the trap the source files set (a
    semicolon inside a string literal), and the guard rails need exact statements.
    """
    rel = str(path.relative_to(SRC))
    trees = parse_file(path)
    rewritten = []
    for tree in trees:
        rewritten.append(rewrite(tree, stats, diagnostics, rel))
    check_alias_comparisons(rewritten, rel, diagnostics)

    statements: list[str] = []
    file_views: dict[str, list[str]] = {}
    for tree in rewritten:
        sql_text = emit_statement(tree, stats, file_views, rel)
        if sql_text:
            statements.append(sql_text.rstrip().rstrip(";"))
    statements.extend(finish_views(file_views, views, stats))

    text = BANNER.format(src=f"sql/{rel}") + "\n" + "\n".join(s + ";\n" for s in statements)
    return text, statements


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default=str(SRC))
    ap.add_argument("--out", default=str(OUT))
    ap.add_argument("--only", help="transpile a single file (relative to sql/)")
    ap.add_argument("--explain", action="store_true",
                    help="EXPLAIN every generated statement against an empty DuckDB")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    src = pathlib.Path(args.src)
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    stats = Stats()
    views: dict[str, str] = {}
    diagnostics: list[str] = []
    problems: list[str] = []
    written: list[pathlib.Path] = []
    all_statements: list[tuple[str, str]] = []

    candidates = sorted(src.rglob("*.sql"))
    if args.only:
        candidates = [src / args.only]

    for path in candidates:
        rel = str(path.relative_to(src))
        if rel in DROPPED:
            if args.verbose:
                print(f"  drop  {rel:44s} {DROPPED[rel]}")
            stats.bump("files dropped")
            continue
        if rel in INCLUDE_DRIVERS:
            if args.verbose:
                print(f"  incl  {rel:44s} include order only")
            stats.bump("files include-driver")
            continue
        try:
            text, stmts = transpile_file(path, stats, views, diagnostics)
        except Exception as e:
            problems.append(f"{rel}: FAILED to transpile: {type(e).__name__}: {e}")
            continue
        dest = out / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text)
        written.append(dest)
        all_statements.extend((rel, s) for s in stmts)
        problems.extend(scan_forbidden(text, rel))
        stats.bump("files written")

    # The manifest is what driver.py consumes: what to run, in what order, and
    # which view becomes which ndjson file.
    output_map = read_output_map()
    include_order = read_include_order("create_fhir_tables.sql")
    manifest = {
        "include_order": [f for f in include_order if f not in DROPPED],
        "views": views,
        "output_files": {views[t]: output_map[t] for t in sorted(views) if t in output_map},
        "dropped": DROPPED,
        "unmapped_views": sorted(set(views) - set(output_map)),
        "unbuilt_outputs": sorted(set(output_map) - set(views)),
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    print(f"\ntranspiled {stats['files written']} files -> {out}")
    print("\nrewrite counts:")
    for k in sorted(stats):
        if k.startswith("files"):
            continue
        print(f"  {stats[k]:5d}  {k}")

    print(f"\nviews: {len(views)}   mapped to ndjson: {len(manifest['output_files'])}")

    # Manifest invariants. driver.py trusts the manifest completely, so a drift
    # here would show up as a missing resource at the end of an 8-hour build.
    if not args.only:
        if manifest["unmapped_views"]:
            problems.append(f"views with no ndjson target: {manifest['unmapped_views']}")
        if manifest["unbuilt_outputs"]:
            problems.append(f"ndjson targets with no view: {manifest['unbuilt_outputs']}")
        if len(views) != len(output_map):
            problems.append(f"expected one view per ndjson output, got {len(views)} "
                            f"views and {len(output_map)} outputs")
        written_rel = {str(p.relative_to(out)) for p in written}
        drift = set(manifest["include_order"]) ^ written_rel
        if drift:
            problems.append(f"include_order and written files disagree: {sorted(drift)}")

    if diagnostics:
        print(f"\ndiagnostics ({len(diagnostics)}):")
        for d in diagnostics:
            print(f"  ? {d}")

    # Syntax check is a DEFAULT guard rail: it needs no tables, so there is no
    # reason to make it opt-in.
    problems.extend(syntax_check(all_statements))

    if args.explain:
        setup = "\n".join([
            "CREATE SCHEMA IF NOT EXISTS fhir_etl;",
            "CREATE SCHEMA IF NOT EXISTS fhir_trm;",
            (REPO / "duckdb_etl" / "runtime" / "10_macros.sql").read_text(),
        ])
        problems.extend(explain_all(all_statements, setup))

    if problems:
        print(f"\nFAILED ({len(problems)} problems):")
        for p in problems:
            print(f"  !! {p}")
        return 1

    print("\nguard rails: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
