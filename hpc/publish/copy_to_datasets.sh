#!/bin/bash
# Copy the three completed FHIR builds from $SCRATCH into the shared team area.
#
#   sbatch hpc/publish/copy_to_datasets.sh
#
# COPY, NOT MOVE. Two reasons, both load-bearing:
#   1. parity-chartevents (30376632) reads fhir-gzip-par as its B side while this
#      runs. A move would delete files out from under a job hours into its work.
#   2. $SCRATCH is Lustre and /datasets is NFS, so mv is copy-then-delete anyway
#      -- the "atomic rename" intuition does not apply across filesystems.
# Removing the scratch copies is a separate, deliberate step for later.
#
# Layout produced (agreed with the user; deliberately NOT the published layout --
# no delta/, no logs/, no parquet.*, just the resource files):
#
#   <DEST>/psql-gzip/fhir/*.ndjson.gz
#   <DEST>/duckdb-gzip/fhir/*.ndjson.gz
#   <DEST>/duckdb-zstd/fhir/*.ndjson.zst
#
#SBATCH --job-name=publish-copy
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=4G
#SBATCH --time=06:00:00
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
set -euo pipefail

DEST_ROOT="${DEST_ROOT:-${DATASETS:?}/work/mimic4}"
NAME="${NAME:-mimic-iv-on-fhir-1.1.3-fixes_2026-08-22}"
DEST="${DEST_ROOT}/${NAME}"
CPUS="${SLURM_CPUS_PER_TASK:-4}"

# name : source : glob
BUILDS=(
    "psql-gzip:${SCRATCH:?}/psql-baseline/out/mimic4-export-gz:*.ndjson.gz"
    "duckdb-gzip:${SCRATCH:?}/duckdb-port/fhir-gzip-par:*.ndjson.gz"
    "duckdb-zstd:${SCRATCH:?}/duckdb-port/fhir-zstd:*.ndjson.zst"
)

echo "host: $(hostname)   job: ${SLURM_JOB_ID:-nojob}   cpus: ${CPUS}"
echo "dest: ${DEST}"
date -Is
START=$(date +%s)

# ---------------------------------------------------------------- preflight
[ -d "$DEST_ROOT" ] || { echo "no such parent: $DEST_ROOT" >&2; exit 2; }
if [ -e "$DEST" ]; then
    echo "destination already exists: $DEST" >&2
    echo "refusing to run -- this lands in a SHARED team area and a partial" >&2
    echo "overwrite there is worse than no copy at all. Remove it deliberately" >&2
    echo "or set NAME to something else." >&2
    exit 2
fi
for spec in "${BUILDS[@]}"; do
    src="${spec#*:}"; src="${src%%:*}"
    [ -d "$src" ] || { echo "no such source: $src" >&2; exit 2; }
done

# Record the sources before touching anything, so the unchanged-check at the end
# compares against a real observation rather than an assumption.
declare -A SRC_BEFORE
for spec in "${BUILDS[@]}"; do
    name="${spec%%:*}"; rest="${spec#*:}"; src="${rest%%:*}"
    SRC_BEFORE[$name]="$(du -sb "$src" | cut -f1) $(ls -1 "$src" | wc -l)"
done

mkdir -p "$DEST"

# ---------------------------------------------------------------- copy
for spec in "${BUILDS[@]}"; do
    name="${spec%%:*}"; rest="${spec#*:}"; src="${rest%%:*}"; glob="${rest#*:}"
    dst="${DEST}/${name}/fhir"
    echo
    echo "=== ${name}"
    echo "    ${src}  ->  ${dst}"
    mkdir -p "$dst"
    t0=$(date +%s)
    # Explicit allow-list, not --exclude of the things we happen to know about:
    # manifest-gz.tsv is the only stray we found, but an allow-list is correct
    # even for strays nobody has looked for yet.
    rsync -a --no-perms --chmod=D2775,F664 \
          --include="$glob" --exclude='*' \
          "${src}/" "${dst}/"
    t1=$(date +%s)
    n=$(find "$dst" -maxdepth 1 -type f | wc -l)
    b=$(du -sb "$dst" | cut -f1)
    echo "    copied ${n} files, ${b} bytes, $((t1-t0))s"
done

# ---------------------------------------------------------------- verify
# rsync verifies its own transfer, but this is a shared area: an independent
# hash, computed by something other than the tool that did the writing, is the
# only check that catches a bug in the writer itself.
echo
echo "=== sha256 verification (independent of rsync)"
FAIL=0
for spec in "${BUILDS[@]}"; do
    name="${spec%%:*}"; rest="${spec#*:}"; src="${rest%%:*}"; glob="${rest#*:}"
    dst="${DEST}/${name}/fhir"
    for side in "$src" "$dst"; do
        ( cd "$side" && find . -maxdepth 1 -name "$glob" -type f -printf '%f\0' \
            | xargs -0 -P "$CPUS" -n 1 sha256sum ) | sort -k2 \
            > "/tmp/sha.$$.$(basename "$side").${name}"
    done
    a="/tmp/sha.$$.$(basename "$src").${name}"
    b="/tmp/sha.$$.$(basename "$dst").${name}"
    if diff -q "$a" "$b" >/dev/null; then
        echo "    ${name}: OK  ($(wc -l < "$a") files match)"
    else
        echo "    ${name}: MISMATCH" >&2
        diff "$a" "$b" | head -20 >&2
        FAIL=1
    fi
    rm -f "$a" "$b"
done
[ "$FAIL" -eq 0 ] || { echo "verification failed -- destination left in place for inspection" >&2; exit 3; }

# ---------------------------------------------------------------- readme
# The directory name cannot carry "unofficial", and should not: status changes,
# and a rename breaks every path anyone wrote down. It goes here instead.
cat > "${DEST}/README.md" <<'README'
# MIMIC-IV-on-FHIR 1.1.3 + fixes -- three builds

**This is not an official release.** It has not been through FHIR validation and
is not a replacement for `mimic-iv-on-fhir-1.1.3_af91a8f/`. Placed here at
<another-user>'s suggestion so the team can look at it.

## What is here

| directory | producer | codec | size |
|---|---|---|---|
| `psql-gzip/fhir/` | PostgreSQL pipeline (the existing build) | gzip | 28.50 GiB |
| `duckdb-gzip/fhir/` | DuckDB port | gzip | 29.24 GiB |
| `duckdb-zstd/fhir/` | DuckDB port | zstd | 24.26 GiB |

Each holds 30 `.ndjson.gz` / `.ndjson.zst` files, one per resource -- the same
shape the IG distributes. No `delta/`, no `logs/`, no `parquet.*`.

## Content is 1.1.3 PLUS three fixes -- do not treat it as 1.1.3

- **#125** carries `chartevents.value` in a `component` when `valuenum` is set.
  This is why `MimicObservationChartevents` is 5,691,409,208 bytes larger
  uncompressed than the published 1.1.3 -- 207.2 bytes across each of 27,464,811
  components.
- **#126** anchors `Patient.birthDate` to the MIMIC anchor pair.
- **#124** generates FHIR tables under UTC to preserve wall-clock timestamps.

Against published 1.1.3, the psql build is byte-identical for 28 of 30
resources; `MimicMedicationRequest` differs by 805 bytes and chartevents by #125.

## The three builds are not byte-identical to each other

Same 555,352,597 records. PostgreSQL `jsonb` emits a space after every colon and
comma; DuckDB emits compact JSON -- 22.5 GiB of whitespace across the corpus, no
content difference. Compare by parsing, not by bytes.

Known real differences from the parity run are documented in `BENCHMARK.md` in
the source repo, along with all timings, per-resource sizes and the method.

## Provenance

| build | job |
|---|---|
| psql | multi-step chain, see BENCHMARK.md section 3 |
| psql -> gzip | 30381616 |
| duckdb-gzip | 30374970 |
| duckdb-zstd | 30378972 |

Copied from `${SCRATCH:?}/` with rsync and verified with independent
sha256sum on both sides.

Contact: the repository owner
README

# ---------------------------------------------------------------- perms
# Match the neighbouring builds: group-writable, setgid so the group is
# inherited. Default umask would land these -rw-r--r-- and leave the team able
# to read but not correct them.
chmod -R g+w "$DEST"
find "$DEST" -type d -exec chmod g+s {} +

# ---------------------------------------------------------------- report
END=$(date +%s)
echo
echo "=== result"
echo "elapsed_seconds=$((END-START))"
du -sh "$DEST"
find "$DEST" -mindepth 1 -maxdepth 2 -type d | sort
echo "total files: $(find "$DEST" -type f | wc -l)"

echo
echo "=== sources unchanged (they must be -- this job only ever read them)"
for spec in "${BUILDS[@]}"; do
    name="${spec%%:*}"; rest="${spec#*:}"; src="${rest%%:*}"
    after="$(du -sb "$src" | cut -f1) $(ls -1 "$src" | wc -l)"
    if [ "$after" = "${SRC_BEFORE[$name]}" ]; then
        echo "    ${name}: unchanged (${after})"
    else
        echo "    ${name}: CHANGED  before='${SRC_BEFORE[$name]}'  after='${after}'" >&2
        FAIL=1
    fi
done
date -Is
exit "$FAIL"
