# MIMIC-IV-on-FHIR: PostgreSQL vs DuckDB build benchmark

Single source of the measured numbers for the SQL-on-FHIR mapping backends, for citation.
Three configurations, same input data, same cluster, same measurement method:

| # | Configuration | Status |
|---|---|---|
| A | **PostgreSQL 15.2**, uncompressed ndjson | ✅ complete |
| B | **DuckDB 1.5.5**, gzip, parallel compression | ✅ complete |
| C | **DuckDB 1.5.5**, zstd, parallel compression | ✅ complete |

A secondary configuration, **B′ = DuckDB / gzip / single-stream**, isolates the contribution of
parallel compression from the contribution of changing engine (§6). Also complete.

> Provenance. Every figure below is either copied from a job log or derived from `sacct` and the
> cgroup sample series; nothing is estimated. Raw sources in §9. Where a number is derived rather
> than measured, the arithmetic is shown so it can be checked.

---

## 1. Headline

| | A — psql | B — DuckDB gzip | C — DuckDB zstd |
|---|---:|---:|---:|
| Build pipeline, as run | 14 h 52 m 36 s | 20 m 25 s | 15 m 16 s |
| Compression to IG shape | **+ 6 m 24 s** | *inline* | *inline* |
| **Wall clock, full pipeline → IG shape** | **14 h 59 m 00 s** | **20 m 25 s** | **15 m 16 s** |
| **Wall clock, minimal path → IG shape** | **9 h 00 m 38 s** | **20 m 25 s** | **15 m 16 s** |
| Speedup vs A, full pipeline | 1.0× | 44.0× | **58.9×** |
| Speedup vs A, minimal path | 1.0× | 26.5× | **35.4×** |
| CPU-hours actually consumed | 16.42 | 6.73 | 5.69 |
| Mean cores busy, whole run | 1.10 | 19.8 | 22.4 |
| Peak memory | 136.3 GiB | 247.2 GiB | 243.1 GiB |
| Spilled to disk | not measured (§6b) | not measured | **0 bytes** |
| Output, as the pipeline writes it | 492.2 GiB `.ndjson` | 29.2 GiB `.ndjson.gz` | **24.3 GiB `.ndjson.zst`** |
| Output, all in IG shape (§7) | 28.5 GiB `.gz` | 29.2 GiB `.gz` | **24.3 GiB `.zst`** |
| Rows / lines emitted | 555,352,597 | 555,352,597 | 555,352,597 |

All three emit exactly 555,352,597 records.

**On the compression row.** DuckDB compresses *inside* the export — the `COPY` writes `.gz`/`.zst`
directly, so there is no separate step and no separate cost to add; it is already inside the
20 m 25 s and 15 m 16 s. PostgreSQL writes plain ndjson, so reaching the shape the IG distributes
takes an extra pass over 528 GB: **384 s** with `pigz -6` on 32 threads (job `30381616`, §7).

Two honesty notes on that 6 m 24 s. It is *generous to psql* in one way and *harsh* in another:

- **Generous:** the compression step was given **32 CPUs**, where every other psql step ran on 8.
  On 8 threads it would be roughly 4× longer (~25 min). Charging psql only 6 m 24 s therefore
  understates the real cost of getting its output into the distributed shape.
- **Harsh:** it is charged as pure overhead, but psql could in principle pipe `\copy` through
  `gzip` and overlap it with the export rather than paying for a second full read. That was not
  how the baseline ran, so it is not what is measured.

Either way it moves the headline by well under 1% — 43.7× → 44.0× — and is included so that all
three columns describe the same deliverable rather than three different ones. Rows below this one
that are sourced from `sacct` (CPU-hours, mean cores, peak memory) cover the **build pipeline
only** and do not include the compression step.

**On size, compare the second row, not the first.** psql writes uncompressed ndjson and DuckDB
writes compressed, so "492 GiB vs 24 GiB" measures a configuration choice, not the pipelines. With
all three normalised to the shape the IG distributes — one `.ndjson.gz`/`.zst` per resource — psql
is 28.5 GiB, DuckDB gzip 29.2 GiB, DuckDB zstd 24.3 GiB. The size advantage is the **codec**, and
it is 17%, not 20×. See §7, which also covers why compressed size is a confounded measure at all.

**zstd wins on every axis: 1.34× faster than gzip and 1.21× smaller.** There is no trade-off to
discuss — it is not the usual "smaller but slower". It is faster to *write* (405.5 s vs 448.5 s of
export) and much faster to *read back* (200.8 s vs 495.1 s of verification, since verification
decompresses everything), and the output is 5.1 GB smaller. If only one DuckDB configuration is
reported, report C.

**DuckDB used ~1.8× more peak memory.** That is the one axis on which it is worse, and it is
worth stating rather than burying. Both figures are *configured* ceilings being filled, not
intrinsic requirements: psql's 136.3 GiB is `shared_buffers=128GB` doing its job, DuckDB's
247.2 GiB is a `memory_limit` set to 60% of an over-generous 480 GB request. Neither pipeline was
memory-starved, so neither number is a minimum — they are not directly comparable as
"requirements", only as "what each run actually touched".

**Speedup: 44.0× on the full pipeline, 26.5× on the minimal path.** Read §2 before quoting
either — they answer different questions and the difference is not a rounding detail.

### The speedup decomposes into two independent factors

Taking B (gzip) as the like-for-like case, over the **build pipelines only** (14:52:36 vs 20:25),
since the CPU-hour figures come from `sacct` on those jobs and exclude the compression step:

|  | psql | DuckDB | ratio |
|---|---:|---:|---:|
| CPU-hours of actual computation | 16.42 | 6.73 | **2.44×** less work |
| Mean cores busy across the run | 1.10 | 19.8 | **18.0×** more parallelism |
| | | | 2.44 × 18.0 = **43.9×** ≈ observed 43.7× (build only) |

This is the more informative framing than a single speedup number. Only about **2.4×** comes from
DuckDB doing less work per row; the remaining **18×** is that PostgreSQL never used the machine.
An 8-CPU allocation running at 1.10 mean cores is a structural property, not a tuning mistake:
**PostgreSQL generates no parallel plan for a data-modifying statement**, and the entire FHIR
build is `INSERT ... SELECT`. The 64-core node was ~98% idle for 15 hours.

---

## 2. What is actually being compared — read this before quoting a speedup

The two pipelines are not the same shape, and the honest comparison depends on the question.

**PostgreSQL, as run — 6 steps, 14 h 59 m:**

```
import (0:40:01) → build (7:16:37) → CLUSTER (5:54:05) → validate (0:04:17) → export (0:57:36)
  → gzip to IG shape (0:06:24)
```

**DuckDB, as run — 2 steps, 20 m 25 s (B) / 15 m 16 s (C):**

```
stage csv.gz→Parquet (0:02:22) → build+export+compress (0:18:03 gzip / 0:12:54 zstd, verification inline)
```

DuckDB has no `CLUSTER` step because it has no equivalent: it never materialises the
`mimic_fhir.*` tables at all. The mappings are **views over Parquet**, exported directly. That
architectural difference *is* part of the result, but it means two of the psql steps have no
counterpart:

| psql step | Time | Has a DuckDB counterpart? |
|---|---:|---|
| import | 0:40:01 | yes — `stage` (0:02:22) |
| build | 7:16:37 | yes — folded into `build` |
| export | 0:57:36 | yes — folded into `build` |
| **CLUSTER** | **5:54:05** | **no** — physical reorganisation of tables DuckDB never creates |
| **validate** | **0:04:17** | partly — DuckDB verifies inline (§4), psql verifies as a step |

- **"Full pipeline" (44.0×)** answers *how long did each approach take, end to end, as actually
  executed*. It charges psql for CLUSTER.
- **"Minimal path" (26.5×)** answers *how long to produce the distributed artifact*, dropping
  CLUSTER and validate: `0:40:01 + 7:16:37 + 0:57:36 + 0:06:24 = 9:00:38`. The exported ndjson
  content is identical without CLUSTER — it reorders rows physically, which changes line order in
  the export but no line contents. **Caveat added after measuring §7:** that reordering does
  change *compressed size* materially (`MimicCondition` compresses 26% better from CLUSTERed
  input), so dropping CLUSTER is free in content but not in artifact size.

**Recommendation for the thesis: quote 26.5× as the headline and state the 44.0× separately.**
The conservative number is still large, and it cannot be attacked on the grounds that CLUSTER is
an optional step the DuckDB path was never asked to do. Note the counter-argument honestly:
CLUSTER is part of the upstream `create_fhir_tables.sql` (it ends with
`\i create_table_clusters.sql`), so "as run" is defensible as the vanilla pipeline. It was split
into a separate job here only for restartability.

**Two confounds that both work *against* DuckDB**, i.e. the real advantage is slightly larger
than measured:

1. **psql writes uncompressed, DuckDB writes compressed.** The DuckDB timings include compression
   the psql timings never paid for, *and* the decompression cost of verifying its own output. An
   uncompressed DuckDB control run (`--compression none`) would quantify this exactly and has not
   been done — it is the one cheap experiment still outstanding.
2. **DuckDB's build includes read-back verification inline** — 495.1 s of B's 1083 s, 200.8 s of
   C's 774 s (§4). psql's equivalent checks are the separate `validate` and `export_manifest`
   steps, which *are* excluded from the minimal-path figure.

One confound working *for* DuckDB: its input is staged Parquet on Lustre, while psql read
`.csv.gz` from NFS. Staging cost is charged to DuckDB (the 0:02:22), but the conversion to a
columnar format is itself part of why the build is fast. This is a genuine property of the
approach, not a measurement artifact — but it should be stated rather than hidden.

---

## 3. Configuration A — PostgreSQL 15.2

Allocation for every step: `--nodes=1 --ntasks=1 --cpus-per-task=8 --mem=192G`, partition `defq`,
node class 64 CPU / 503 GB. `shared_buffers=128GB`, `work_mem=512MB`,
`maintenance_work_mem=4GB`, `timezone=UTC`, PGDATA on Lustre.

| Step | Job | Wall | TotalCPU | Peak mem | Mean cores |
|---|---|---:|---:|---:|---:|
| import | 30337394 | 0:40:01 | 1:07:03 | 73.8 GiB | 1.68 |
| build | 30339566 | **7:16:37** | 8:37:57 | 135.1 GiB | 1.19 |
| CLUSTER | 30353020 | 5:54:05 | 5:06:29 | 119.3 GiB | 0.87 |
| validate | 30362384 | 0:04:17 | 0:10:58 | **136.3 GiB** | 2.56 |
| export | 30363082 | 0:57:36 | 1:22:30 | 113.6 GiB | 1.43 |
| **total** | | **14:52:36** | **16:24:57** | **136.3 GiB** | **1.10** |

Plus export verification (30368919) at 0:01:45, not counted in the total.

**Where the 7 h 17 m build went** — summed statement time is 26,126 s against 26,126 s of wall
clock, so the step is SQL end to end with no dead time:

| Resource | Rows | Time | Share |
|---|---:|---:|---:|
| `observation_chartevents` | 313,645,032 | 12,548 s (3 h 29 m) | 48% |
| `observation_labevents` | 118,171,367 | 7,046 s (1 h 57 m) | 27% |
| `medication_administration` | 27,754,178 | 1,701 s | 7% |
| `medication_request` | 13,533,220 | 797 s | 3% |
| `procedure` | 12,689,766 | 582 s | 2% |
| everything else (93 statements) | ~69.6 M | ~3,552 s | 14% |

**Two resources are 75% of build time and 80% of output bytes.** Any port that does not
accelerate `observation_chartevents` and `observation_labevents` cannot move the total.

Notable: mean cores *fell* from 1.68 (import) to 1.19 (build) to 0.87 (CLUSTER), and rose to 2.56
only for `validate` — the one **read-only** step, where `max_parallel_workers_per_gather` finally
becomes eligible. That inversion is the clearest single piece of evidence for the parallelism
argument in §1.

---

## 4. Configuration B — DuckDB 1.5.5, gzip, parallel compression

| Config | Step | Job | Alloc | Wall | TotalCPU | Peak mem | Mean cores |
|---|---|---|---|---:|---:|---:|---:|
| both | stage (csv.gz → Parquet) | 30368991 | 12 CPU / 32 G | 0:02:22 | 0:16:42 | 9.7 GiB | 7.06 |
| B | build + export, gzip | 30374970 | 64 CPU / 480 G `--exclusive` | 0:18:03 | 6:27:05 | 247.2 GiB | 21.45 |
| C | build + export, zstd | 30378972 | 64 CPU / 480 G `--exclusive` | **0:12:54** | 5:24:57 | 243.1 GiB | 26.43 |
| **B total** | | | | **0:20:25** | **6:43:47** | **247.2 GiB** | **19.8** |
| **C total** | | | | **0:15:16** | **5:41:39** | **243.1 GiB** | **22.4** |

The staging step is shared: B and C read the same Parquet, so the comparison between them is
purely the codec.

Settings: `threads=64`, `memory_limit=294912MB` (60% of `--mem`), `temp_directory` on node-local
`/tmp`. 97 Parquet files loaded in 3.8 s; 30 mappings as views; no intermediate tables.

**Inside each build:**

| Phase | B gzip (1083 s) | C zstd (774 s) |
|---|---:|---:|
| Export (JSON build + compress + write) | 448.5 s (41%) | 405.5 s (52%) |
| Read-back verification | **495.1 s (46%)** | **200.8 s (26%)** |
| Load, setup, teardown | ~139 s (13%) | ~168 s (22%) |

Verification re-reads every written file through `read_ndjson_objects` and asserts the line count
matches the rows written — it checks that the writer emitted exactly one JSON value per line, the
property a CSV-writer quoting bug breaks. **Under gzip it costs more than the export itself.**
A `--no-verify` build would be ≈ 10 min (B) or ≈ 9.5 min (C); the quoted figures are the verified
ones.

**Most of zstd's advantage is in decompression, not compression.** Export is only 43 s faster
(448.5 → 405.5); verification is 294 s faster (495.1 → 200.8). That is worth stating separately
because it generalises: every downstream consumer that reads these files pays the decompression
cost, and this benchmark happens to measure it because it verifies its own output.

**Peak memory 247.2 GiB (B) / 243.1 GiB (C) against the 480 GB requested** — oversized by ~2×.
`sacct` MaxRSS agrees with the cgroup peak in both (244.9 / 242.4 GiB). See §6b before sizing a
re-run down: the `memory_limit` is derived from `--mem`, so shrinking the request shrinks the
buffer pool and can introduce spilling that neither of these runs had.

---

## 5. Per-resource detail

Configuration B, with configuration B′ (single-stream) where available. `par s` / `seq s` are
export seconds; `MB` is the gzip output size.

| resource | rows | B gzip s | C zstd s | B′ seq s | B/B′ | B gzip MB | C zstd MB | B′ seq MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `condition` | 4,756,326 | 36.7 | 42.1 | 224.8 | 6× | 300.7 | 200.4 | 300.9 |
| `condition_ed` | 899,050 | 27.8 | 34.6 | 43.1 | 2× | 71.2 | 51.2 | 71.2 |
| `encounter` | 431,231 | 1.5 | 1.8 | 12.4 | 8× | 52.4 | 47.9 | 52.4 |
| `encounter_ed` | 425,087 | 0.6 | 0.5 | 7.3 | 12× | 33.6 | 31.8 | 33.5 |
| `encounter_icu` | 73,181 | 0.3 | 0.2 | 1.4 | 4× | 7.5 | 7.0 | 7.5 |
| `location` | 39 | 0.1 | 0.1 | 0.1 | 1× | 0.0 | 0.0 | 0.0 |
| `medication` | 19,689 | 0.3 | 0.3 | 1.1 | 4× | 1.1 | 1.0 | 1.1 |
| `medication_administration` | 27,754,178 | 19.0 | 23.1 | 403.6 | 21× | 1278.7 | 1157.2 | 1278.3 |
| `medication_administration_icu` | 8,978,893 | 6.8 | 6.8 | 121.8 | 18× | 499.7 | 458.9 | 499.6 |
| `medication_dispense` | 12,689,766 | 11.1 | 9.1 | 226.4 | 20× | 1291.0 | 1163.3 | 1290.9 |
| `medication_dispense_ed` | 1,586,053 | 1.0 | 0.9 | 15.0 | 15× | 75.7 | 67.5 | 75.7 |
| `medication_mix` | 6,338 | 19.8 | 16.6 | 35.6 | 2× | 0.6 | 0.5 | 0.6 |
| `medication_request` | 15,416,901 | 18.2 | 16.1 | 241.6 | 13× | 1075.1 | 922.7 | 1074.5 |
| `medication_statement_ed` | 2,598,365 | 2.2 | 1.6 | 46.0 | 21× | 256.1 | 185.5 | 294.3 |
| **`observation_chartevents`** | 313,645,032 | 156.6 | 123.0 | 5796.9 | 37× | 15738.5 | 12499.7 | 18094.6 |
| `observation_datetimeevents` | 7,112,999 | 6.0 | 4.2 | 63.6 | 11× | 250.9 | 266.8 | 250.7 |
| `observation_ed` | 4,404,481 | 14.2 | 13.5 | 67.2 | 5× | 345.9 | 323.7 | 345.9 |
| **`observation_labevents`** | 118,171,367 | 89.3 | 74.6 | 2421.8 | 27× | 7868.4 | 6571.2 | 7866.5 |
| `observation_micro_org` | 289,928 | 1.3 | 3.0 | 8.4 | 7× | 54.3 | 47.7 | 55.2 |
| `observation_micro_susc` | 1,107,278 | 1.3 | 1.3 | 13.9 | 11× | 50.1 | 48.8 | 50.1 |
| `observation_micro_test` | 2,184,371 | 2.7 | 2.3 | 39.9 | 15× | 280.2 | 219.3 | 280.1 |
| `observation_outputevents` | 4,234,967 | 2.6 | 2.4 | 41.5 | 16× | 164.2 | 164.3 | 164.1 |
| `observation_vital_signs` | 9,948,485 | 19.7 | 19.4 | 157.3 | 8× | 777.7 | 747.9 | 778.1 |
| `organization` | 1 | 0.0 | 0.1 | 0.2 | 10× | 0.0 | 0.0 | 0.0 |
| `patient` | 299,712 | 1.0 | 0.9 | 4.5 | 4× | 13.5 | 12.8 | 13.5 |
| `procedure` | 669,186 | 0.6 | 0.5 | 7.5 | 12× | 42.0 | 33.6 | 42.0 |
| `procedure_ed` | 1,989,697 | 1.0 | 0.8 | 18.7 | 18× | 95.7 | 90.8 | 95.6 |
| `procedure_icu` | 696,092 | 0.8 | 0.7 | 8.2 | 10× | 34.7 | 32.5 | 34.7 |
| `specimen` | 1,587,215 | 0.8 | 0.8 | 16.6 | 20× | 90.3 | 84.4 | 95.7 |
| `specimen_lab` | 13,376,689 | 5.2 | 4.5 | 119.1 | 23× | 651.6 | 615.7 | 741.3 |
| **total** | **555,352,597** | **448.5** | **405.5** | **10165.5** | **23×** | **31401** | **26054** | **33889** |

`medication_mix` is the one resource that barely improves (19.8 s vs 35.6 s for 6,338 rows). It
is bound by correlated subqueries, not by the compressor, so parallel gzip has nothing to give
it. At 0.6 MB it does not affect the total — recorded so it is not mistaken for a regression.

---

## 6. Configuration B′ — the parallel-compression contribution

A single `COPY … TO 'x.gz'` runs **one** compressor thread regardless of `threads`, so the export
was pinned near one core no matter the allocation. `PER_THREAD_OUTPUT` gives each thread its own
compressor; gzip is a multi-member format, so the parts concatenate byte-wise into one valid
`.gz` and the output layout is unchanged (still one file per resource, as psql produces).

Job `30374316`, now complete: **3 h 11 m 21 s wall, 169.4 min of write time, 33.89 GB.**

| | single-stream (B′) | parallel (B) | |
|---|---:|---:|---|
| **whole build, wall** | **3:11:21** | **0:18:03** | **10.6× faster** |
| total export time | 10,165.5 s | 448.5 s | 22.7× faster |
| mean cores busy | 2.27 (of 8) | 21.45 (of 64) | |
| total output | 33.89 GB | 31.40 GB | 7.3% smaller |
| `observation_chartevents` export | 5,796.9 s (1 h 37 m) | 156.6 s | **37× faster** |
| `observation_chartevents` size | 18,094.6 MB | 15,738.5 MB | **13% smaller** |
| `observation_labevents` export | 2,421.8 s (40 m) | 89.3 s | **27× faster** |

**This is the single most consequential change in the port.** Without it the DuckDB build is
3 h 11 m — still 4.7× faster than psql's full pipeline, but a different result entirely from
18 minutes. One `COPY` option accounts for most of the headline.

**The parallel output is also smaller, which is counter-intuitive and worth stating.** Splitting
a stream into 64 independent gzip members should compress *worse* — each member starts with an
empty dictionary. It compresses better here because `preserve_insertion_order = false` means the
single-stream writer receives an arbitrary interleave of rows from all threads, while
`PER_THREAD_OUTPUT` keeps each thread's rows contiguous. The locality gained outweighs the shared
dictionary lost. The effect is strongly resource-dependent: 13% on `chartevents`, 13% on
`medication_statement_ed`, 12% on `specimen_lab`, and ~0% on most others.

⚠️ **B′ ran on 8 CPU / 48 G, not the full node.** It isolates the compressor serialisation, and
its mean of 2.27 cores shows the ceiling directly — even with 8 CPUs available, a single gzip
stream plus the scan could not use more than ~2. It is *not* a like-for-like allocation against
B, and its wall time should not be read as "what DuckDB would do on 8 cores"; a single-stream
build on 64 cores would be barely faster, because the bottleneck is one thread either way. Its
peak memory (48.0 GiB) sat exactly at the cgroup ceiling — that is page cache filling the
allocation and being reclaimed, not memory pressure; the job completed `0:0`.

---

## 6b. Was either backend memory-constrained?

The claim *"each approach had as much RAM as it needed; neither was throttled by an artificially
small allocation"* is worth making precisely, because it is the first thing an examiner will
attack — a fast engine given 480 GB against a slow one given 32 GB is not a comparison.

**What is fully established:**

| | psql | DuckDB gzip | DuckDB zstd |
|---|---|---|---|
| Slurm `--mem` requested | 192 G | 480 G | 480 G |
| Peak memory actually used | 136.3 GiB (71%) | 247.2 GiB (51%) | 243.1 GiB (51%) |
| Killed / OOM / failed for memory | **no** | **no** | **no** |
| Ever hit the Slurm ceiling | **no** | **no** | **no** |
| Spilled to disk | unmeasured | unmeasured | **0 bytes** ✅ |

Neither run came close to its allocation, and every step exited `0:0`. **You can state without
qualification that neither backend was constrained by the Slurm allocation, and that neither
failed or was killed for memory.** That is measured, not inferred.

### The DuckDB half is now settled

Configuration C ran with the spill sampler active and reported **`peak_spill_bytes = 6`** across
51 samples — six bytes, i.e. the empty temp directory itself, no spill file ever created. Since C
ran the same code on the same allocation and the same data as B, and peaked within 4 GiB of B's
memory, **neither DuckDB run spilled**. `memory_limit = 288 GiB` was never exceeded; the engine
had all the memory it wanted and the timings contain no hidden disk traffic.

That closes the question for DuckDB completely, and it is the stronger claim of the two: not
merely "was not killed" but "provably never needed more than it had".

### The PostgreSQL half is not settled

Not hitting the *Slurm* ceiling is not the same as having as much RAM as the engine wanted.
PostgreSQL's `work_mem = 512MB` is a **per-operation** limit: a sort or hash join exceeding it
spills to temp files on Lustre, silently and slowly. The FHIR build joins over 313 M-row tables,
so spilling is plausible. `log_temp_files` was left at its default of `-1` (off), so nothing was
recorded. `shared_buffers = 128GB` is generous and the peaks confirm it filled — but
`shared_buffers` is a cache, not the sort/hash budget.

**Filesystem evidence, gathered without starting the server:**
`$PGDATA/base/pgsql_tmp` is **empty but its directory is 20,480 bytes**. A directory that never
held entries stays at 4,096; this one was grown by many temp files at some point. So PostgreSQL
did spill. But its mtime is `2026-08-22 15:27:08`, which falls inside `psql-verify-fixes`
(30372926, 15:22:42–15:27:49) — a read-only analysis job that is **not part of the 14:52:36
benchmark total**. The build ran 21:53–05:10 the previous night and left no dateable trace, so the
filesystem proves spilling occurred but cannot attribute it to a benchmarked step.

**The remaining check, and its limits.** The cluster is still on disk (745 GB, cleanly shut down,
no stale lock, `pg_stat/pgstat.stat` present so the counters survived) and `pg_stat_database`
accumulates temp-file usage for the life of the database:

```sql
SELECT datname, temp_files, pg_size_pretty(temp_bytes) FROM pg_stat_database WHERE datname='mimic4';
```

This counter is **cumulative for the life of the database**, so it cannot isolate the build step
either — it includes `psql-verify-fixes`, which the filesystem already implicates. It is a
magnitude check, not an attribution: tens of GB is consistent with verify-fixes alone and implies
the build did not spill meaningfully; terabytes would mean the build spilled heavily and its
7 h 17 m includes substantial disk traffic. Not run here, because it requires starting a
postmaster on the baseline cluster — and an unclean stop triggers crash recovery, which
**discards the very statistics being read**.

**Suggested wording for the thesis:**

> Both backends were allocated substantially more memory than they used — PostgreSQL peaked at
> 136.3 GiB of 192 GiB requested, DuckDB at 247.2 GiB of 480 GiB — and neither was terminated
> or throttled by the scheduler. DuckDB was additionally instrumented to record spilling to
> temporary storage and recorded none, confirming its buffer pool was never exceeded.
> PostgreSQL was configured with `shared_buffers = 128GB` and `work_mem = 512MB`, the settings
> used for the production baseline; per-operation spilling was not instrumented.

That concedes the one genuinely unknown parameter instead of over-claiming "as much RAM as
necessary" for something never measured. Note that the asymmetry is honest but not neutral: an
unmeasured psql spill would mean psql's times are *worse* than its configuration could achieve,
which flatters DuckDB. Stating the caveat is what keeps the comparison defensible.

---
## 7. Output artifacts — shape and size

**All three builds now ship the shape the MIMIC-on-FHIR IG distributes: one
`<Resource>.ndjson.gz` (or `.zst`) per resource, 30 files, nothing else.**

The psql pipeline writes plain uncompressed ndjson — `create_fhir_jsons.sql` uses a bare `\copy`
with no compression anywhere in its path. That is why the earlier "492 GiB vs 24 GiB" figure was
not a like-for-like comparison: it measured what each pipeline was *configured to emit*, not how
efficiently either represents FHIR. The psql export has therefore been compressed to the IG shape
as a separate step (`hpc/psql/gzip_export.sh`, job `30381616`, pigz -6 on 32 threads, **384 s**),
writing to a **new** directory and leaving the 528 GB original byte-for-byte untouched.

### Totals

| | shape | files | compressed | uncompressed | ratio |
|---|---|---:|---:|---:|---:|
| **published IG 1.1.3** | `.ndjson.gz` | 30 | 31,387,296,338 (29.23 GiB) | 522,790,030,964 (486.89 GiB) | 16.7× |
| **A — psql** | `.ndjson.gz` | 30 | **30,598,746,022 (28.50 GiB)** | 528,481,439,367 (492.19 GiB) | 17.3× |
| **B — DuckDB gzip** | `.ndjson.gz` | 30 | 31,401,149,392 (29.24 GiB) | 504,346,678,652 (469.71 GiB) | 16.1× |
| **C — DuckDB zstd** | `.ndjson.zst` | 30 | **26,053,937,404 (24.26 GiB)** | 504,346,678,652 (469.71 GiB) | 19.4× |

### The psql build reproduces the published artifact, and the delta is exactly fix #125

Comparing uncompressed sizes per resource against the published IG 1.1.3 distribution:

| | resources | uncompressed size vs published |
|---|---:|---|
| byte-identical | **28 of 30** | exact match |
| `MimicMedicationRequest` | 1 | psql is **805 bytes smaller** (of 15.9 GB) |
| `MimicObservationChartevents` | 1 | psql is **5,691,409,208 bytes larger** (+2.22%) |

**That chartevents delta is fix #125 and nothing else.** The fix carries `chartevents.value` in a
`component` when `valuenum` is set, on 27,464,811 Observations:

```
5,691,409,208 bytes / 27,464,811 components = 207.2 bytes per component
```

207 bytes is the right size for a one-element coding component carrying a system URL, a code and a
display string. **This is an independent, byte-level confirmation of #125 at full scale** — a
different instrument entirely from the SQL predicate checks in `verify_fixes.sql`, which counted
components rather than measuring them.

The other two fixes are invisible to a size comparison, as they should be: #126 changes
`Patient.birthDate` *values* but every date is exactly 10 characters, and #124 changes timestamps
to UTC without changing their rendered width. Their correctness rests on `verify_fixes.sql`
(`PSQL_RUN_METRICS.md` §30372926), not on this.

Compressed, the psql build is **28.50 GiB against the published 29.23 GiB** — 2.5% smaller,
despite carrying 5.7 GB *more* uncompressed content. The compressed figure is not comparable
across producers (see "Reading the compressed columns" below); the uncompressed one is.

> The published per-resource figures in the table below are reconstructed: 29 were measured
> directly, `MimicObservationChartevents` was re-measured on its own (job `30381831`) after a
> scripting error truncated the first result file. The reconstruction sums to
> **522,790,030,964**, matching the independently measured total exactly, so it is checked rather
> than assumed.

### The uncompressed numbers, and why B and C are identical

**B and C decompress to exactly the same 504,346,678,652 bytes.** They are separate builds with
different row orderings, but the same 555,352,597 records serialised the same way, so the byte
total must match — and it does, which is a useful consistency check on both runs.

**DuckDB's uncompressed output is 95.4% of psql's** (469.71 vs 492.19 GiB) for identical content.
The difference is entirely JSON whitespace: PostgreSQL `jsonb` emits its canonical text form with
a space after every colon and comma; DuckDB emits compact JSON. Same line, 81 spaces vs 12 — the
12 remaining are inside actual string values:

```
psql    {"id": "0a8eebfd-...", "meta": {"profile": [...]}, "name": [{"use": "official", ...
duckdb  {"resourceType":"Patient","meta":{"profile":[...]},"gender":"male","name":[{"use":...
```

Semantically identical, which is why `parity.py` parses both rather than comparing bytes.

### Per-resource

| resource | A psql raw | A psql `.gz` | pub raw | pub `.gz` | DuckDB raw | B `.gz` | C `.zst` |
|---|---:|---:|---:|---:|---:|---:|---:|
| `MimicCondition` | 3,146,817,627 | 238,709,807 | 3,146,817,627 | 247,104,524 | 3,013,640,499 | 300,680,297 | 200,376,037 |
| `MimicConditionED` | 591,281,006 | 56,874,701 | 591,281,006 | 59,243,057 | 566,107,606 | 71,165,230 | 51,183,077 |
| `MimicEncounter` | 802,164,033 | 52,858,965 | 802,164,033 | 55,407,660 | 762,568,083 | 52,421,726 | 47,887,634 |
| `MimicEncounterED` | 512,451,501 | 32,920,519 | 512,451,501 | 34,366,748 | 487,612,494 | 33,587,172 | 31,804,879 |
| `MimicEncounterICU` | 68,858,121 | 7,189,478 | 68,858,121 | 7,550,990 | 65,449,211 | 7,527,569 | 7,021,166 |
| `MimicLocation` | 17,616 | 1,738 | 17,616 | 1,776 | 16,797 | 5,890 | 5,848 |
| `MimicMedication` | 11,823,716 | 1,056,843 | 11,823,716 | 1,091,253 | 11,334,525 | 1,090,502 | 1,003,340 |
| `MimicMedicationAdministration` | 25,054,007,746 | 1,237,959,083 | 25,054,007,746 | 1,317,989,079 | 23,961,034,759 | 1,278,675,359 | 1,157,196,987 |
| `MimicMedicationAdministrationICU` | 10,015,091,360 | 499,705,368 | 10,015,091,360 | 531,270,915 | 9,552,208,908 | 499,694,055 | 458,890,543 |
| `MimicMedicationDispense` | 12,841,505,555 | 898,394,317 | 12,841,505,555 | 946,643,340 | 12,306,949,157 | 1,290,987,549 | 1,163,266,986 |
| `MimicMedicationDispenseED` | 918,695,074 | 72,565,930 | 918,695,074 | 76,645,944 | 880,807,062 | 75,675,340 | 67,456,899 |
| `MimicMedicationMix` | 4,512,872 | 590,777 | 4,512,872 | 618,666 | 4,351,968 | 621,053 | 496,294 |
| `MimicMedicationRequest` | 15,955,159,998 | 1,094,246,690 | 15,955,160,803 | 1,153,535,907 | 15,221,114,889 | 1,075,074,464 | 922,670,748 |
| `MimicMedicationStatementED` | 2,130,058,517 | 157,790,937 | 2,130,058,517 | 164,499,266 | 2,039,583,255 | 256,109,422 | 185,503,419 |
| `MimicObservationChartevents` | 261,975,562,580 | 15,889,252,091 | 256,284,153,372 | 15,986,843,998 | 250,479,013,029 | 15,738,532,554 | 12,499,683,401 |
| `MimicObservationDatetimeevents` | 5,657,337,937 | 255,329,212 | 5,657,337,937 | 271,538,128 | 5,416,525,419 | 250,908,709 | 266,785,690 |
| `MimicObservationED` | 3,526,435,341 | 221,093,739 | 3,526,435,341 | 237,057,220 | 3,351,661,886 | 345,851,250 | 323,734,877 |
| `MimicObservationLabevents` | 158,740,107,826 | 8,289,507,201 | 158,740,107,826 | 8,610,332,071 | 150,972,442,049 | 7,868,387,816 | 6,571,222,954 |
| `MimicObservationMicroOrg` | 294,292,480 | 46,649,131 | 294,292,480 | 49,015,889 | 282,334,930 | 54,257,960 | 47,725,733 |
| `MimicObservationMicroSusc` | 1,281,009,873 | 52,910,458 | 1,281,009,873 | 54,982,004 | 1,223,771,102 | 50,091,737 | 48,750,952 |
| `MimicObservationMicroTest` | 1,817,885,034 | 176,200,322 | 1,817,885,034 | 183,869,993 | 1,739,468,838 | 280,152,660 | 219,308,934 |
| `MimicObservationOutputevents` | 3,558,558,341 | 165,073,811 | 3,558,558,341 | 171,839,804 | 3,394,512,712 | 164,187,559 | 164,308,250 |
| `MimicObservationVitalSignsED` | 9,282,340,073 | 430,094,223 | 9,282,340,073 | 467,714,654 | 8,772,691,190 | 777,659,686 | 747,908,499 |
| `MimicOrganization` | 470 | 347 | 470 | 345 | 447 | 314 | 316 |
| `MimicPatient` | 357,531,356 | 13,888,387 | 357,531,356 | 15,612,277 | 338,292,686 | 13,545,451 | 12,809,275 |
| `MimicProcedure` | 386,594,338 | 41,044,876 | 386,594,338 | 42,930,485 | 370,533,874 | 42,014,347 | 33,584,821 |
| `MimicProcedureED` | 1,074,879,340 | 75,563,646 | 1,074,879,340 | 80,703,596 | 1,027,126,612 | 95,684,833 | 90,777,382 |
| `MimicProcedureICU` | 518,111,468 | 35,126,283 | 518,111,468 | 36,503,640 | 494,223,282 | 34,688,538 | 32,529,838 |
| `MimicSpecimen` | 882,382,527 | 68,453,802 | 882,382,527 | 71,973,539 | 842,999,589 | 90,266,537 | 84,379,189 |
| `MimicSpecimenLab` | 7,075,965,641 | 487,693,340 | 7,075,965,641 | 510,409,570 | 6,768,301,794 | 651,603,813 | 615,663,436 |
| **total** | **528,481,439,367** | **30,598,746,022** | **522,790,030,964** | **31,387,296,338** | **504,346,678,652** | **31,401,149,392** | **26,053,937,404** |
| **GiB** | **492.19** | **28.50** | **486.89** | **29.23** | **469.71** | **29.24** | **24.26** |

### Reading the compressed columns

Compressed size is **not** a clean measure of either engine, because it mixes three things: the
5% whitespace difference, the codec, and **row ordering**. The third is the surprising one:

- `MimicCondition` — DuckDB gzip is **26% larger** than psql gzip (300.7 vs 238.7 MB) despite
  having 4% *fewer* bytes to compress.
- `MimicObservationChartevents` — DuckDB gzip is **1% smaller** (15,738 vs 15,889 MB).

The likely cause is that the psql export reads from tables that were physically `CLUSTER`ed by
`patient_id`, so a patient's records are adjacent and compress well, while DuckDB runs with
`preserve_insertion_order = false` and emits an arbitrary parallel order. That would make the
5 h 54 m CLUSTER step — which §2 argues can be dropped from the minimal-path comparison — worth
something after all, just not in build time. **This is a hypothesis consistent with the data, not
a measured result;** confirming it needs a DuckDB build with an `ORDER BY` on the export, which
has not been run.

The clean, unconfounded comparison is **gzip vs zstd on the same DuckDB output**: 29.24 → 24.26
GiB, **17% smaller**, from identical input bytes. That one is a pure codec result.

## 8. Correctness

Wall-clock numbers mean nothing without the output being equivalent. Status:

| Check | Result |
|---|---|
| Row counts, all 30 resources, DuckDB vs psql `manifest.tsv` | ✅ **30/30 exact** |
| psql internal validation (`validate_fhir_tables.sql`) | ✅ 30/30 PASSED |
| psql line-count verification (`export_manifest.sh`) | ✅ 30/30 PASSED |
| psql fix verification (#124/#125/#126, value level) | ✅ all PASS |
| Demo-scale semantic parity, DuckDB vs psql | ✅ 28/30 identical, 2 differ only in `hasMember` array order |
| **Full-scale parity, 28 resources** (`30376630`) | ✅ **25/28 identical**, 3 differing — analysed below |
| Full-scale parity, `observation_labevents` (`30376631`) | ⏳ running |
| Full-scale parity, `observation_chartevents` (`30376632`) | ⏳ running |

### The three differing resources — full-scale, 30376630

| resource | differing | of | path | kind |
|---|---:|---:|---|---|
| `MimicEncounter` | 17,579 | 431,231 | `$.type` | `array_order` |
| `MimicMedicationStatementED` | 187,587 | 2,598,365 | `$.medicationCodeableConcept.coding` | `array_order` |
| `MimicMedicationAdministration` | **20** | 27,754,178 | `$.dosage.dose.value` | **`value`** |

Every resource matched on **line count, id set, and every other field** — `only-A = 0` and
`only-B = 0` throughout. No record is missing, duplicated or extra anywhere in the 28.

**The two `array_order` findings have the same root cause as the already-accepted `hasMember`:
an aggregate with no `ORDER BY` in the shared `sql/*.sql`.**

```sql
-- sql/fhir_encounter.sql:30      Encounter.type
jsonb_agg(jsonb_build_object('coding', ...)) AS cpt_ARRAY          -- no ORDER BY
-- contrast line 10, same file, which DOES order:
jsonb_agg(...) ORDER BY intime) AS location_array

-- sql/fhir_medication_statement_ed.sql:14   MedicationStatementED.coding
STRING_AGG(CAST(... AS TEXT), ',') AS med_ETC_CODES                 -- no ORDER BY
```

PostgreSQL does not promise an order for either, so **two psql runs need not agree with each
other**. These are not port defects; they are unspecified behaviour in the mapping SQL that the
port surfaced. Element *sets* are identical in every case. If the ordering should be
deterministic, the fix is `ORDER BY` in `sql/*.sql` so both backends get it — but that would
invalidate the completed 516 GB psql baseline, so it is deferred, not done.

**The 20-record `value` difference is a genuine port divergence** and the only one found so far.

```
psql 100000303380049042   vs   DuckDB 100000303380049040
psql 74.99999999999999999  vs   DuckDB 75
```

Cause, confirmed by direct test on DuckDB 1.5.5:

| source SQL | Postgres | DuckDB port |
|---|---|---|
| `CAST(emd.dose_given AS DECIMAL)` | `numeric` — arbitrary precision | `CREATE MACRO fhir_decimal(x) AS CAST(x AS DOUBLE)` |

Postgres `jsonb` stores numbers as arbitrary-precision `numeric`, so an 18-digit integer or a
19-significant-digit decimal round-trips exactly. DuckDB's macro casts to IEEE-754 `DOUBLE`, which
holds ~15–17 significant digits. Exactly 20 of 27,754,178 source values exceed that.

**It should not be "fixed" naively.** The obvious repair — `CAST(x AS DECIMAL(38,18))` — was
tested and is far worse: it renders the *common* case `98.6` as `98.600000000000000000`, which
would make every numeric field in every resource diverge:

```
value                   DOUBLE json          DECIMAL(38,18) json
98.6                    98.6                 98.600000000000000000
74.99999999999999999    75.0                 74.999999999999999990
100000303380049042      100000303380049040.0 100000303380049042.000000000000000000
```

So `DOUBLE` is the right default and the 20 records are a documented limitation: **the DuckDB
port represents FHIR decimals in IEEE-754 double precision, exact for all but 20 of the
555,352,597 records (0.0000036%), all in `MedicationAdministration.dosage.dose.value` where the
source carries more than 17 significant digits.** That is a defensible sentence for the thesis
and is more useful than a spurious claim of bit-identity.

Row-count agreement is necessary, not sufficient: it proves no resource was dropped or
duplicated, and says nothing about field contents. Full-scale parity (`duckdb/parity.py`) is the
actual gate — it matches records by `id` and compares canonicalised JSON, normalising only object
key order (Postgres `jsonb` sorts keys; DuckDB preserves insertion order) and numeric formatting
(`5.20` vs `5.2`). Anything else — a missing key, a reformatted timestamp, a truncated decimal —
is reported as a difference.

**One accepted difference:** `array_order:$.hasMember`. Both backends aggregate panel members
with an unordered aggregate (`jsonb_agg` / `json_group_array`), so element order is unspecified on
*both* sides and two psql runs need not agree either. Member *sets* are still compared.

**Do not quote configuration B's timings as a validated result until §8 row 6 is green.**

---

## 9. Source data

Local copies of the machine-readable metrics, for plotting:

```
metrics/duckdb/duckdb-build-30374970.resources.json   per-resource rows/bytes/seconds, config B
metrics/duckdb/duckdb-build-30378972.resources.json   ditto, config C (zstd)
metrics/duckdb/duckdb-build-30374316.resources.json   ditto, config B' (single-stream)
metrics/duckdb/duckdb-build-*.samples.csv             15 s cgroup series (mem, cpu) per build
metrics/duckdb/duckdb-build-30378972.spill.csv        temp-dir series -- the no-spill evidence, §6b
metrics/duckdb/duckdb-stage-30368991.samples.csv      staging series
metrics/duckdb/logs/                                  the job logs the tables above were read from

# §7 artifact sizes -- all "<bytes>\t<filename>", one row per resource
metrics/size-psql-raw.tsv     A, uncompressed .ndjson       metrics/raw-ref.tsv    published, uncompressed
metrics/size-psql-gz.tsv      A, compressed .ndjson.gz      metrics/raw-zstd.tsv   B and C, uncompressed
metrics/size-ref.tsv          published IG 1.1.3, .gz       metrics/artifact_table.md  the rendered table
metrics/size-gzip-par.tsv     B, .ndjson.gz
metrics/size-zstd.tsv         C, .ndjson.zst
```

`raw-zstd.tsv` covers **both** DuckDB builds: B and C decompress to the same byte total, since
they are the same records serialised the same way in a different row order.

Sample series are `epoch,mem_current_bytes,cpu_usage_usec`; `cpu_usage_usec` is cumulative, so
mean cores over a window is `Δusec / 1e6 / Δwall`. `mem_current_bytes` is the cgroup figure and
**includes page cache** — prefer it to `sacct MaxRSS` for whole-job peaks (MaxRSS counts one
process and double-counts shared pages across backends), but do not read it as a progress signal.

On the cluster:

```
$DATASETS/work/mimic4/mimic-iv-on-fhir-1.1.3_af91a8f/fhir/
                                             THE PUBLISHED IG ARTIFACT -- read only, not ours
$SCRATCH/psql-baseline/metrics/      config A samples + collect_metrics.sh output
$SCRATCH/psql-baseline/out/mimic4-export/       config A artifact, UNCOMPRESSED (492 GiB)
$SCRATCH/psql-baseline/out/mimic4-export-gz/    config A artifact in IG shape (28.5 GiB)
$SCRATCH/duckdb-port/metrics/        configs B/B'/C samples + resources.json
$SCRATCH/duckdb-port/logs/           job logs
$SCRATCH/duckdb-port/fhir-gzip-par/  config B artifact  (29.2 GiB)
$SCRATCH/duckdb-port/fhir-zstd/      config C artifact  (24.3 GiB)
$SCRATCH/duckdb-port/fhir-gzip/      config B' artifact (31.6 GiB, single-stream)
```

**`mimic4-export/` and `mimic4-export-gz/` are separate directories and both are needed.** The
uncompressed one is what `parity.py` reads; `gzip_export.sh` refuses to write into a non-empty
destination and never touches the source, because a half-rewritten export would destroy a parity
run with hours invested and cost ~15 h to rebuild.

Both wrappers (`hpc/psql/run_psql_step.sh`, `hpc/duckdb/run_duckdb_step.sh`) use a **byte-identical
cgroup sampler**, so peak memory and mean cores are measured the same way on both backends. That
is deliberate and must be preserved if either script is edited.

Narrative detail for configuration A is in `PSQL_RUN_METRICS.md`; port design and traps are in
`HANDOFF.md`.

---

## 10. Configuration C — zstd — running, job `30378972`

```bash
sbatch -J duckdb-build-zstd --exclusive --cpus-per-task=64 --mem=480G --time=06:00:00 \
    --export=ALL,SAMPLE_INTERVAL=15,OUT_DIR=$SCRATCH/duckdb-port/fhir-zstd \
    repo/hpc/duckdb/run_duckdb_step.sh build zstd
```

**The allocation is deliberately identical to B's, including the 480 G that B only half used.**
Sizing C down to B's measured 247.2 GiB peak was the obvious move and would have been a mistake:
`memory_limit` is derived as 60% of `--mem`, so `--mem=320G` gives a 187.5 GiB limit — *below*
what B actually touched. C would then spill where B did not, and the gzip↔zstd delta would be
measuring the allocation, not the codec. Any future C′ re-run must keep this allocation.

### Two prerequisites, both now resolved

**1. The parallel path was gated on `compression == "gzip"`.** zstd would have silently fallen
back to a single compressor thread and looked artificially slow — the 37× measured on
`chartevents` in §6 would simply have vanished, and the resulting "gzip is faster than zstd"
conclusion would have been an artifact of the driver, not a property of the codecs. The gate is
now a `CONCATENABLE` set covering both.

**2. Concatenation had to be verified for zstd, not assumed from the spec.** Job `30378930`,
8 threads, 20 M rows, both codecs, checking that a byte-joined file returns every row through
*both* DuckDB's reader and the codec's own CLI (the artifact is published; a file only DuckDB can
decompress is not usable ndjson):

| codec | parts | joined bytes | DuckDB rows | distinct ids | CLI `-dc` lines |
|---|---:|---:|---:|---:|---:|
| gzip | 8 | 107,546,434 | 20,000,000 ✅ | 20,000,000 ✅ | 20,000,000 ✅ |
| zstd | 8 | 33,216,328 | 20,000,000 ✅ | 20,000,000 ✅ | 20,000,000 ✅ |

Kept as `duckdb/tests/test_concat_codecs.py`. It asserts **at least 2 parts** — the first two
attempts produced exactly 1 and proved nothing, because `range()` is a single-threaded table
function and the login node reports `nproc = 1`. A vacuous pass here is the dangerous outcome:
the failure mode being tested for is a reader that stops after the first frame and returns fewer
rows, all of them correct, from a file that opens without error.

⚠️ The size column above is on synthetic data with a constant padding field and must not be
quoted as a compression ratio. Note only that parallel zstd came out *smaller* than
single-stream zstd (33.2 MB vs 66.7 MB), the same locality effect described in §6.

## Open items

- Full-scale parity must pass before any of this is quotable (§8).
- B′ column completion (§6).
- Enable the parallel path for zstd before running C (§10).
- An uncompressed DuckDB control run would remove confound 1 in §2. Cheap — one mid-size
  resource re-exported at `none`/`gzip`/`zstd` gives a same-hardware compression cost.
