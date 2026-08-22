# Porting the FHIR mappings off psql (DuckDB / Spark)

> **Status: scratch notes, not a decision record.** Findings from a feasibility pass over
> `sql/` on 2026-08-21. Verified claims are marked ✓ and were run against DuckDB v1.5.5
> locally. Everything else is estimate or judgement. Delete or fold into `README.md` once
> the direction is settled.

**Verdict: feasible, and DuckDB is the right primary target — not Spark.**
The mapping SQL is structurally portable; the friction is a small fixed vocabulary of
Postgres-specific *function calls*, which is exactly what a deterministic AST rewriter
handles well. No hand-editing of the 37 mapping files required.

---

## 1. What the code actually looks like

37 files, ~4,250 lines under `sql/`. Census of Postgres-specific constructs
(`grep -roh` over `sql/*.sql`, `sql/fn/*.sql`, `sql/fhir_etl/*.sql`):

| Construct | Files | Occurrences |
|---|---|---|
| `jsonb_build_object` | 32 | 406 |
| `jsonb_build_array` | 29 | 162 |
| `jsonb_strip_nulls` | 27 | 33 |
| `json_build_object` (non-b) | 6 | 9 |
| `uuid_generate_v5` | 29 | 194 |
| `CAST(... AS TIMESTAMPTZ)` | 23 | 45 |
| `fhir_etl.fn_*` calls | 31 | 32 |
| psql meta-commands (`\i`, `\echo`, `\copy`) | 4 | ~270 |
| `jsonb_agg` / `json_agg` | 2 / 1 | 3 / 1 |
| window functions (`OVER (`) | 2 | 3 |
| `DISTINCT ON`, `LATERAL`, `generate_series` | 0 | 0 |

**Key finding:** the mapping bodies are boring, portable ANSI SQL — CTEs, `LEFT JOIN`
against tiny lookup tables, `CASE`, one `jsonb_each_text` unnest
(`fhir_observation_vitalsigns.sql`), three window functions. There is **no correlated-subquery
or procedural logic inside the mappings**. The zero rows in that table matter as much as the
large ones: no `DISTINCT ON`, no `LATERAL`, nothing that needs restructuring.

The 5 plpgsql functions in `sql/fn/` are the only genuinely procedural code, and they are
called from a single expression position each.

---

## 2. Why the current pipeline is slow

Diagnosis matters here, because it determines whether a port actually helps. It is *not*
primarily that Postgres is bad at JSON — it's the pipeline shape:

1. **`INSERT ... SELECT` is single-threaded on the insert side.** Postgres can parallelize
   the SELECT, but not the insert. Most cores sit idle. This is the main reason for the
   ~12h figure in `README.md` despite a 24-core box.
2. **Every jsonb blob is WAL-logged, TOAST-compressed, written, then read back.**
3. **`create_table_clusters.sql` does a full rewrite + sort of all 24 tables**, including
   the 300M-row chartevents/labevents ones.
4. **`\copy` reads it all back out through one client connection.**

DuckDB does `source → ndjson` in one parallel streaming pass and **deletes steps 2–4
entirely**. The intermediate `mimic_fhir.*` tables and the `CLUSTER` step exist only to
serve the ndjson export and HAPI loading; writing Parquet partitioned/sorted by
`patient_id` gives the clustering for free as a side effect of the write.

Expectation (estimate, not measured): **12h → well under 1h** single-node, dominated by
source I/O rather than JSON construction. The spike in §8 exists to replace this guess
with a number.

---

## 3. The three things that do not transpile

### 3.1 `uuid_generate_v5` — SOLVED, pure SQL, no UDF ✓

This was the item most likely to force a slow per-row Python UDF. It does not.
Verified against the RFC 4122 test vector:

```sql
CREATE MACRO uuid_v5(ns, name) AS (
  WITH h AS (SELECT sha1(from_hex(replace(ns::VARCHAR,'-','')) || encode(name)) AS s)
  SELECT substr(s,1,8)||'-'||substr(s,9,4)||'-5'||substr(s,14,3)||'-'
      || format('{:x}', ((('0x'||substr(s,17,1))::INT & 3) + 8))
      || substr(s,18,3)||'-'||substr(s,21,12)
  FROM h
);
```

Reproduce:

```sh
duckdb -c "<macro above>
SELECT uuid_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8','www.example.com') AS got,
       '2ed6657d-e927-568b-95e1-2665a8aea6a2' AS want;"
-- got == want ✓
```

Fully vectorized. Covers all 194 call sites. `sql/fhir_etl/uuid_namespace.sql` also uses
`uuid_ns_oid()` — that is the constant `6ba7b810-9dad-11d1-80b4-00c04fd430c8`'s OID sibling
`6ba7b812-9dad-11d1-80b4-00c04fd430c8`; hardcode it, or better, **precompute the whole
namespace table once and commit it as a CSV** since it is 31 fixed rows that never change.

The same `sha1`/`unhex`/`concat` trick works in Spark.

### 3.2 `jsonb_strip_nulls` — real semantic gap, mechanically fixable ✓

DuckDB has no equivalent (checked the full `duckdb_functions()` JSON list — no
`json_strip_nulls`). Two things verified:

- `json_merge_patch(x, '{}')` does **not** strip nulls. ✗
- `json_merge_patch('{}', x)` **does** strip top-level nulls ✓ — but JSON Merge Patch
  never recurses into arrays, whereas Postgres `jsonb_strip_nulls` **does**.

That gap is live, because the code is full of `jsonb_build_array(jsonb_build_object(...))`
with nullable leaves.

**Fix:**

```sql
CREATE MACRO strip(j) AS json_merge_patch('{}'::JSON, j);
```

**Where to apply it — this is narrower than it first looks.** Merge Patch *does* recurse
into nested objects, arbitrarily deep; it only refuses to recurse into arrays. Verified ✓:

```sql
json_merge_patch('{}', {'a':{'b':NULL,'c':1}, 'd':NULL})  -- {"a":{"c":1}}         ✓ recursed
json_merge_patch('{}', {'a':{'b':{'c':NULL,'d':1}}})      -- {"a":{"b":{"d":1}}}   ✓ 2 deep
json_merge_patch('{}', {'a':[{'b':NULL,'c':1}]})          -- {"a":[{"b":null,...}]} ✗ array
```

So `strip()` is needed **only at the top level and on objects that are direct elements of an
array** (recursively, for arrays inside those). That is `jsonb_build_array(jsonb_build_object`
= **116 sites**, not all 409. Most are small leaf codings, so the parse/serialize happens over
tiny documents.

Verified equivalent ✓ — strip-at-116-sites and strip-at-all-409-sites produce **byte-identical**
output over a 5M-row synthetic FHIR Observation build (`cmp` clean).

```sql
SELECT strip(json_object(
   'resourceType','Patient',
   'gender', NULL,
   'identifier', json_array(strip(json_object('value','123','system',NULL)))
));
-- {"resourceType":"Patient","identifier":[{"value":"123"}]}
```

⚠ **DuckDB's `JSON` logical type is `VARCHAR` internally** (`duckdb_types()` confirms), so
every `json_merge_patch` is a parse → merge → serialize round-trip rather than an in-place
mutation. I expected the strip-everywhere variant to be meaningfully slower for this reason.
**It was not** — 6.5s vs 7.8s over 5M rows, i.e. within noise and in the *opposite* direction.
So minimal placement is the right rule for clarity and lower allocation churn, but do not
expect a speedup from it, and do not contort the rewriter to achieve it.

Note the parity-preserving edge case: an object that becomes **empty** after stripping
yields `{}` in both Postgres and DuckDB. Same behaviour, so parity holds — but `{}` is
invalid in most FHIR positions, so if the HAPI validator currently complains about any of
these, it will complain identically after the port. Not a regression; don't chase it.

Doing this by hand across 406 `jsonb_build_object` sites would be miserable. As an AST rule
it is ~10 lines. **This is the single strongest argument for the rewriter over a hand port.**

Spark note: `to_json` with `ignoreNullFields=true` (the default) does recursive
null-stripping natively, so on Spark this rule collapses to a no-op.

### 3.3 The 5 plpgsql functions — hand-port, ~1 day

`sql/fn/`: `fn_patient_extension`, `fn_med_statement`, `fn_build_medication_identifier`,
`fn_prescriptions_medication_code`, `fn_create_table_patient_dependent`.

All except the last build JSON arrays by **string concatenation with manual comma
placement** and `::TEXT` casts. They are the most fragile code in the repo. They become:

```sql
list_filter([...], x -> x IS NOT NULL)   -- DuckDB
array_compact(array(...))                -- Spark
```

which is both clearer and faster. This is the one place worth writing real unit tests,
including the `med_GSN = '0'` path in `fn_med_statement` (leading `CASE ... THEN '['`
has no `ELSE`, so the whole concatenation collapses to `NULL` — appears intentional,
but pin it with a test before changing anything, and preserve the behaviour exactly).

`fn_create_table_patient_dependent` simply disappears — no tables, no indexes.

---

## 4. Tooling: sqlglot AST rewriter

Not regex. Not hand-editing. `sqlglot.parse(sql, read="postgres")` → apply rules →
`.sql(dialect=...)`.

| # | Rule | Sites |
|---|---|---|
| 1 | `jsonb_build_object(...)` → `json_object(...)`, wrapped in `strip()` **only** at top level + array-element positions (see §3.2) | 409 rewritten, 116 wrapped |
| 2 | `jsonb_build_array(...)` → `json_array(...)` | 162 |
| 3 | `jsonb_strip_nulls(x)` → `x` (redundant after rule 1) | 33 |
| 4 | `uuid_generate_v5(...)` → `uuid_v5(...)` | 194 |
| 5 | `CAST(x AS TIMESTAMPTZ)` → `fhir_instant(x)` | 45 |
| 6 | `fhir_etl.fn_*` → `fn_*` macro | 32 |
| 7 | `INSERT INTO mimic_fhir.X SELECT ...` → `COPY (SELECT ...) TO 'X.ndjson'` | 30 |
| 8 | Drop `CREATE TABLE` / `CLUSTER` / `fn_create_table_patient_dependent(...)` | ~55 |

~8 rules, ~800 call sites. Rule 5 is quietly the most valuable: it **centralizes all 45
timestamp renderings into one macro**, which is where the parity diffs will otherwise live.

Orchestration (`\i`, `\echo`, `\copy`, `\set`, the `\if :{?outputdir}` guard in
`create_fhir_jsons.sql`) moves into a small Python driver — which you want anyway for
SLURM job control.

**Keep `sql/*.sql` (Postgres dialect) as the source of truth and transpile at build time.**
Do not do a one-time conversion. There is active mapping-bug work in flight on this branch
(`debug-patient-birthdate-anchor/`); a forked copy would need every fix applied twice.
Suggest generated output to `build/duckdb/` and gitignored, or committed with a
"generated — do not edit" banner if you want the diffs reviewable.

---

## 5. Export

DuckDB writes ndjson natively — no need for the
`\copy ... WITH CSV QUOTE AS E'\b' DELIMITER E'\001'` escaping hack in
`create_fhir_jsons.sql`.

`COPY (...) TO 'f.ndjson' (FORMAT JSON)` wraps each row as an object of its columns
(`{"fhir":{...}}`), which is not what you want. To emit the bare resource per line,
verified ✓ including embedded quotes and commas:

```sql
COPY (SELECT fhir::VARCHAR AS x FROM ...)
TO 'f.ndjson' (FORMAT CSV, HEADER false, QUOTE '', DELIMITER E'\x01');
```

Round-trips through `json.loads` cleanly. Same trick as psql, but DuckDB gives proper
control over quoting rather than relying on `\b` never appearing in the data.

### Compression: **gzip is the standard option**, zstd an opt-in addon

**DECIDED (2026-08-21):** the output format stays as it is today. **gzip is the default**,
for ecosystem compatibility — every downstream consumer reads it without special handling,
and it matches how PhysioNet data is normally distributed. zstd is exposed as an opt-in flag
for internal/intermediate use, not as the default. Plain uncompressed ndjson must remain
selectable.

Make this a single pipeline setting, not a per-resource choice:

```sql
COPY (SELECT fhir::VARCHAR AS x FROM ...)
TO 'f.ndjson.gz' (FORMAT CSV, HEADER false, QUOTE '', DELIMITER E'\x01',
                  COMPRESSION 'gzip');   -- 'zstd' | 'none' via config
```

Measured on the 5M-row synthetic build (10-core laptop):

| Compression | Wall time | Output size | Status |
|---|---|---|---|
| none | 4.67 s | 2.51 GB | must stay selectable |
| **gzip** | **16.46 s** | **0.09 GB** | **default** |
| zstd | 5.05 s | 0.07 GB | opt-in addon |

**Know the cost you are accepting:** gzip is ~3.5× the uncompressed write time, and
**that penalty does not parallelize away** — verified at 24.2s / 16.7s / 17.1s for 1 / 4 / 10
threads, i.e. it plateaus at ~4 threads exactly like the uncompressed path. zstd achieves a
better ratio at ~8% cost.

This is a fine trade in context — the stage takes minutes, not the ~12h the psql pipeline
takes, so 3.5× on it is affordable and compatibility is worth more. But it does mean
compression, not I/O, becomes the write-stage bottleneck (see §7.3). If the full-scale run
later proves write-stage-bound, switching the default to zstd is a one-line config change,
and the measurements above are the justification.

⚠ The 36× ratio is inflated — the synthetic fixture is highly repetitive (`i%13`, `i%977`).
Real FHIR ndjson is still very compressible (endlessly repeated profile URLs, system URIs and
JSON keys), so expect roughly **8–15×**. Measure it in the Phase-1 spike rather than trusting
either number. Either way `README.md`'s **~2 TB free space** requirement drops to ~150–250 GB.

---

## 6. Parity suite

`sql/fhir_etl/subjects.sql` (100 ICU patients, `LIMIT 100`) is the natural fixture — note
it is currently **defined but not actually referenced** by any mapping in the full build,
so it needs wiring in as a filter for the test path.

**Compare semantically, not byte-for-byte.** Postgres `jsonb` sorts object keys by
length-then-bytes; DuckDB preserves insertion order. Same FHIR, different bytes. Parse both
sides to dicts and diff. Byte-identical output against the published PhysioNet ndjson would
need a separate key canonicalizer — probably not worth it, but it is a real decision to make
consciously if anyone downstream diffs the files.

Three diff sources to watch, all centralized by the rewriter:

- **Timestamps.** Postgres renders jsonb `timestamptz` as `2180-07-23T14:00:00+00:00`.
  The `fhir_instant` macro must match exactly. Bonus: DuckDB sidesteps the DST trap the
  `README.md` warns about, because the macro formats UTC explicitly rather than depending on
  a session `TimeZone` setting that a caller could override.
- **Numerics.** `NUMERIC` vs `DOUBLE` render differently (`1.0` vs `1`). Use `DECIMAL`.
- **Empty-after-strip objects.** See §3.2 — parity-preserving, ignore.

---

## 7. Memory consumption — expect a real increase, driven by *threads* not data

> **Target node: 64 CPU / 503 GB RAM.** At that ratio the conclusion is:
> **memory is a non-issue, and this section is mostly about not shooting yourself in the
> foot with defaults.** See §7.4 for the concrete sizing.

Postgres today is frugal and effectively un-OOM-able here: it streams, bounded by
`shared_buffers` + `work_mem` per operation, spilling to disk without complaint. DuckDB
uses far more — but 64 × ~0.3 GB/thread ≈ **19 GB against 503 GB available (~4%)**, so the
headroom is roughly 25×. The theoretical regression does not bite on this hardware.

### 7.1 Measured (caveat: synthetic, laptop, single run)

5M-row synthetic FHIR Observation build → 2.5 GB ndjson, on a 10-core / 16 GB macOS laptop.
Treat absolute numbers as indicative only; the *shape* is what transfers.

| Threads | Wall time | Peak RSS |
|---|---|---|
| 1 | 12.0 s | **0.12 GB** |
| 4 | 4.8 s | **1.99 GB** |
| 10 | 5.4 s | **3.07 GB** |
| 64 (extrapolated) | — | **~19 GB of 503 GB** |

**Two conclusions, both counter-intuitive:**

1. **Peak memory scales roughly linearly with thread count (~0.3 GB/thread), not with data
   volume.** Each thread carries its own operator state and JSON/write buffers. Extrapolated
   to 64 threads: **~19 GB**. On a 503 GB node, irrelevant.
2. **Throughput plateaued at ~4 threads** because the stage is *write-bound* (2.5 GB out).
   Going 4 → 10 threads cost 1 GB of RAM for no speedup. **This, not memory, is the thing to
   engineer around** — see §7.3.

### 7.2 Two HPC gotchas worth pinning down early

**`memory_limit` does not bound process RSS.** Verified: with `SET memory_limit='1GB'`, peak
RSS was still **2.4 GB**. The limit governs DuckDB's buffer manager, not total allocation
(JSON string building and CSV writer buffers sit outside the tracked pool). So a SLURM
`--mem` must be set *well above* `memory_limit`, not equal to it.

**DuckDB's default `memory_limit` is 80% of *system* RAM, and it reads the node, not the
cgroup.** On this box it defaulted to 12.7 GiB of 16 GB; on the target node it will default
to ~400 GB. **If you request a partial node from SLURM, DuckDB still sees all 503 GB** and
will happily exceed the job's cgroup allocation → **OOM-kill by the scheduler, not a clean
DuckDB error.** Set `threads` and `memory_limit` explicitly from the allocation. On a *full*
node reservation this is harmless; on a partial one it is the single most likely cause of a
confusing first failure.

Also verified: setting the limit too low (`256MB`) produced a clean
`Out of Memory Error` rather than spilling — the COPY writer needs a floor. And `1GB`
was *slower* than `4GB` (13.3s vs 8.0s) from spill pressure. Under-provisioning costs speed;
over-provisioning costs nothing here.

### 7.3 The actual bottleneck: write bandwidth, not RAM

On a 64-core / 503 GB node the job is **I/O-bound long before it is memory-bound**. The
`README.md` ~2 TB output figure is the problem, not the working set. Two levers, in order:

1. **Compress the output (§5).** An estimated 8–15× volume reduction, which is what makes
   the ~2 TB figure tractable. **gzip is the decided default**, at ~3.5× the uncompressed
   write time — so with gzip the write stage becomes *compression-bound* rather than
   I/O-bound, and it plateaus at ~4 threads (measured). zstd would give the same reduction at
   ~8% cost and is available as an addon if the full-scale run proves this stage dominant.
2. **Stage source data as Parquet on node-local scratch, not the shared FS.** MIMIC-IV is
   ~100 GB uncompressed CSV, maybe 20–30 GB as Parquet — which **fits in RAM 15× over**.
   Read it once, never touch the parallel FS on the hot path.

With 503 GB you can also run the whole thing against an in-memory database (`:memory:`)
rather than a `.db` file, removing a storage layer entirely. Worth trying in the spike.

### 7.4 Concrete starting configuration for a 64/503 node

```sql
SET threads = 32;             -- start here, not 64; raise only if profiling shows CPU-bound
SET memory_limit = '300GB';   -- generous; spilling is what costs time, not headroom
SET preserve_insertion_order = false;  -- big win for parallel scan→write, order is irrelevant here
SET temp_directory = '<node-local scratch>';
```

`threads = 32` rather than 64 is deliberate: throughput plateaued at ~40% of cores in the
laptop test, and oversubscribing a write-bound pipeline just adds contention. Raise it after
step 1 above, once compression has actually removed the I/O ceiling. `preserve_insertion_order
= false` is the one setting most likely to be left on the table — ndjson line order carries no
meaning here.

The pipeline is genuinely streaming (scan → project → JSON → write) with broadcast joins
against tiny lookup tables, so there is no large hash table or shuffle to hold. The few
non-streaming spots, none of which are close to concerning at 503 GB:

- `fhir_observation_vitalsigns.sql` — `jsonb_each_text(to_jsonb(vs))` unnest multiplies rows
- `fhir_patient.sql` — `ROW_NUMBER() OVER (PARTITION BY subject_id)` in the `ed_race` CTE
  requires a sort
- `fhir_specimen.sql` and friends — `GROUP BY` on a few million distinct ids

**Spark comparison — withdrawn.** I previously flagged Spark's more predictable behaviour
under a hard memory cap as its one genuine advantage. At 64 cores / 503 GB that argument
evaporates: the workload needs ~4% of available RAM, so there is no cap to degrade against.
DuckDB is now the clear choice on every axis for this node.

---

## 8. Recommended sequencing

**Phase 1 — spike (2–3 days).** Runtime macro library (`uuid_v5`, `strip`, `fhir_instant`)
+ three resources chosen to cover the shape space: `fhir_patient` (flat), `fhir_specimen`
(grouped/aggregated), `fhir_observation_labevents` (high-volume). Plus the parity harness on
the 100-patient set. **Deliverable: a measured speedup number on the real hardware**, which
replaces the estimate in §2 and decides whether the rest is worth doing.

**Phase 2 — full port (1–2 weeks).** Remaining ~30 resources, 5 function ports,
orchestration driver replacing the psql meta-commands.

**Phase 3 — Spark backend (~3 days, optional).** Only if data outgrows a single node.

---

## 9. Why not Spark (and the hedge)

Look at the workload shape: scan one big fact table, broadcast-join a handful of tiny lookup
tables, compute a hash, build a JSON string, write. **It is map-only.** No wide shuffles, no
big-to-big joins. Nothing here needs a distributed engine — it needs cores and no WAL.

The 64 CPU / 503 GB node settles it: MIMIC-IV as Parquet (~20–30 GB) **fits in RAM roughly
15× over**, and the pipeline's peak working set is ~4% of the node. There is no capacity
argument for distributing this. Adding a second node would add coordination cost to a job
that never needs to exchange data between partitions.

Against that, Spark on a SLURM allocation costs a standalone cluster brought up inside the
job, JVM heap tuning, and shuffle spill on shared scratch — real operational pain for a
workload with no shuffle. `PSQL_REBUILD_CSIRO.md` marks `SPARK_CONF_DIR` as TBP, so that
setup cost is being paid from zero.

**The hedge is still cheap**, and that is the point of the architecture: the rewriter emits a
dialect and the runtime macros are ~30 lines per backend. Keep the seam honest — nothing
backend-specific outside the macro library and the generator — so Spark stays a few days away
rather than a rewrite. But on current evidence **do not build it**. Given the node spec, the
realistic trigger for Spark is not MIMIC-IV at all; it would be applying this pipeline to a
substantially larger dataset later.

Spark's one remaining advantage is §3.2 — native recursive null-stripping via
`ignoreNullFields`, which makes rewrite rule 1 a no-op. That is a code-simplicity point,
not a performance one.

---

## 10. Open questions for whoever picks this up

- Is byte-identical parity with the published PhysioNet ndjson a requirement, or is semantic
  parity enough? (§6 — changes whether a key canonicalizer is needed.)
- Do the intermediate `mimic_fhir.*` tables have consumers other than `create_fhir_jsons.sql`
  and the HAPI load path? If so, Parquet output needs to serve them.
- Does the HAPI validation path (`py_mimic_fhir`) read from Postgres directly, or only from
  the exported ndjson? If the former, it needs a shim.
- Does the write stage actually dominate at full scale? If so, revisit the gzip default —
  zstd is the same reduction at ~8% cost instead of 3.5× (§5). Not urgent; one-line change.
- Is node-local scratch available on the HPC cluster, and how large? (§7.3 — staging source Parquet
  locally rather than on the shared FS is the second-biggest I/O lever.)
- What is the actual write bandwidth to the target output FS? That, not CPU or RAM, sets the
  floor on total runtime.

**Answered:** output format stays as-is with **gzip as the standard compression option** and zstd as an opt-in addon (§5). Target node is **64 CPU / 503 GB RAM** — single-node DuckDB confirmed as the
right shape (§7.4, §9).
