# Scale the GPM IMERG HH virtual Icechunk build

Status: **implemented and validated at month scale.** This doc started as a plan; it now
records what we built, why, and what we learned tuning it. Year-scale is the next test.

## The core constraint

The pipeline (3-stage SQS + Lambda, `design.md`) writes virtual references for **486,480
granules** (1998-01-01 → 2025-10-01, every 30 min) into one Icechunk store. Each granule is
a disjoint `time` slice.

**Commit throughput is the ceiling, not per-file work.** An Icechunk commit is a
compare-and-swap on the `main` branch tip, so commits *serialize* — only one batch lands at
a time. Two consequences drive everything below:

- **Raising `MAX_CONCURRENCY` does not raise throughput.** More Lambdas just queue at the
  commit point (and pile load on the Earthdata auth endpoint — see below). The real
  throughput lever is **fewer, larger commits**, i.e. a bigger `SQS_BATCH_SIZE`.
- **Per-file processing is cheap and network-bound** (~0.5 s/granule — HDF5 header
  range-reads from GES DISC). Memory is a non-issue: a "chunk" is byte-range metadata, not
  array data.

The manifest is split **one year per shard** (`open_or_create_repo` uses
`manifest_split_size = 48 * 365 = 17,520`). This matters for scaling: each commit rewrites
the shard(s) its batch touched, and a shard's rewrite cost grows as it fills.

## What we changed (and why)

### 1. Rebase-resolve commit — `Processor.commit_processed_files` (processor.py)

The original bare `session.commit()` meant any concurrent commit conflict failed the whole
batch → SQS redelivery → eventually DLQ. We added a bounded, jittered-backoff retry loop
that rebases on `ConflictError` and retries the commit.

The important subtlety: **the rebase solver must *resolve*, not just *detect*.**
`ConflictDetector` only detects — on any conflict it raises `RebaseFailedError`
("Snapshot cannot be rebased. Aborting rebase"). We instead use:

```python
session.rebase(icechunk.BasicConflictSolver(
    on_chunk_conflict=icechunk.VersionSelection.UseOurs
))
```

Why this is needed and safe: granules *usually* write disjoint slices, but SQS is
at-least-once and the dispatch is resumable/redrivable, so the **same** granule (same
`time` index, same chunks) can be processed by two batches concurrently. Two such commits
racing produce a `ChunkDoubleUpdate`, which `ConflictDetector` refuses. Because a duplicate
region write is **byte-identical** to the original (same source byte ranges), "ours" and
"theirs" are the same bytes — `UseOurs` resolves it cleanly. Anything `UseOurs` can't
resolve (a structural / Zarr-metadata conflict) re-raises `RebaseFailedError`; we log the
conflicting `conflict_type`/`path` and propagate so the batch redelivers (retrying wouldn't
help). Attempt exhaustion re-raises the `ConflictError` for the same reason.

### 2. Cache the Earthdata credential provider — `helpers.py`

Reading a source granule makes obstore mint temporary S3 credentials from the Earthdata
`s3credentials` endpoint. Building a fresh `NasaEarthdataCredentialProvider` + `S3Store`
**per granule** defeated obstore's credential cache, so the endpoint was hit once per
granule → bursts that intermittently failed with `UnauthenticatedError` (surfaced as a
wrapped `SystemError` crossing the Rust/Python boundary). We memoize both with
`@lru_cache(maxsize=1)`, so one provider + store are reused across the whole batch in a warm
container, hitting the endpoint roughly once per credential lifetime.

**Caveat that bounds concurrency:** the cache is *per process*. It does nothing across
containers, so the auth-endpoint load scales with `MAX_CONCURRENCY`, not with how many
granules each container reads. (See the `MAX_CONCURRENCY=1000` lesson below.)

### 3. Infrastructure hardening + tunable knobs — `cdk/`

- **Queue retention** raised to 14 days (`cdk/stack.py`) — the default 4 days would silently
  drop messages on a multi-day backfill.
- **Tunable knobs in `cdk/settings.py`** so tuning = redeploy, no code edits:
  `LAMBDA_TIMEOUT`, `LAMBDA_MEMORY`, `VISIBILITY_TIMEOUT`, `MAX_CONCURRENCY`,
  `SQS_BATCH_SIZE`, plus `SQS_MAX_BATCHING_WINDOW`.
- **SQS batching window** (`SQS_MAX_BATCHING_WINDOW`, default 5 s, wired into the
  `SqsEventSource`): AWS **requires** a batching window ≥ 1 s once `SQS_BATCH_SIZE > 10`.
  A `_check_batching_window` validator enforces this; `_check_visibility_timeout` enforces
  `VISIBILITY_TIMEOUT ≥ LAMBDA_TIMEOUT`.

## Operational lessons (learned the hard way)

- **`VISIBILITY_TIMEOUT ≥ LAMBDA_TIMEOUT`, and let CDK own it.** Hand-editing the SQS
  visibility timeout to 60 s while the Lambda ran longer caused in-flight messages to become
  visible again and be reprocessed *concurrently* → a flood of duplicate same-slice writes →
  the rebase conflicts above (>100 in 10 min). The settings validator guarantees the
  invariant for CDK-managed deploys; don't bypass it in the console. AWS best practice is
  ~6× the Lambda timeout for redrive headroom. **Current `300/300` holds the invariant but
  has zero margin — bump `VISIBILITY_TIMEOUT` toward 900–1800 before pushing batch size.**
- **Keep `MAX_CONCURRENCY` moderate (~50).** At 50 we saw ~10 transient auth errors over a
  full week (self-healing). At **1000** the Earthdata auth endpoint was overwhelmed by ~1000
  simultaneous cold-container credential fetches → widespread `UnauthenticatedError`. Since
  concurrency buys no throughput (commits serialize) and *aggravates* both auth load and
  rebase churn, there's no reason to go high.
- **Batch size is the throughput lever, capped by `LAMBDA_TIMEOUT`.** Bigger batches mean
  fewer serialized commits *and* fewer credential fetches per granule (one cached token
  amortized over the batch). The cap: `batch_size × per_granule_time` must finish within the
  timeout with margin.

## Measured numbers (early in the build, shard nearly empty)

| Config (`conc` / `batch`) | Lambda duration | Throughput | Notes |
|---|---|---|---|
| 50 / 25 | ~12 s/batch | 1 week (336) in ~6 min | ~10 transient auth errors |
| 50 / 100 | well under timeout | 1 week in ~2 min; 1 month (~1,440) in ~10 min | DLQ 0 |

Per-granule ≈ 0.5 s. At `batch 100` that's ~50 s/batch — a ~25× margin under the 300 s
timeout, so batch size can grow substantially **without** raising the timeout or memory
(`LAMBDA_MEMORY=4096`). Current deployed config: `MAX_CONCURRENCY=50`, `SQS_BATCH_SIZE=100`,
`SQS_MAX_BATCHING_WINDOW=5`, `LAMBDA_TIMEOUT=300`, `VISIBILITY_TIMEOUT=300`.

## Scaling regimes — why small-scale numbers over-promise

Week and month tests run in a **linear "under-saturated" zone** and will *under-count* the
full build. Two effects only appear at year scale:

1. **Concurrency saturation.** A month is ~15 batches (`batch 100`) against 50 slots —
   fully parallel, lots of idle capacity. A year is ~175 batches → ~3–4 waves on 50
   concurrency, *and* all 175 commits serialize on `main`. Small tests never exercise this.
2. **Shard fill.** A month fills only ~8% of one annual shard, so commits stay cheap the
   whole time. A year fills a shard 0 → 17,520 refs, and each commit rewrites it, so commit
   duration **climbs** across the run.

So a clean ~8–10 min month confirms "still in the linear zone," **not** that the full build
is linear. Rough throughput model: `total ≈ max(reads: granules·per_granule/concurrency,
commits: (granules/batch_size)·commit_cycle)`. At scale the commit term dominates and grows
with shard fill; bigger batches shrink it.

**Next test: a full year (1998, 17,520 msgs).** Watch in CloudWatch: total time, the
**commit-duration trend** as the shard fills (the key signal), Lambda duration vs timeout,
memory headroom, and DLQ depth (must stay 0). Because each year fills its own shard from
empty, the per-year cost curve roughly repeats — so a year's curve is what to extrapolate
the ~28-year build from, not a week's average. If commit cost keeps climbing toward year-end,
raise `SQS_BATCH_SIZE` (fewer commits per shard) before running the whole thing.

Advanced lever (only if single-branch commit throughput proves unacceptable after tuning):
Icechunk's distributed `Session.fork`/merge commits many region writes at once, but it needs
a coordinator and doesn't fit the per-batch-Lambda model — a separate execution path, out of
scope unless the year-scale runbook shows we need it.

## Dispatch script — `scripts/dispatch.py`

Pure enumeration + SQS sends; no HDF5 reads, no AWS compute. Runs locally with
`sqs:SendMessage` and the queue URL.

- Iterates `t` over half-open `[--start, --end)` in 30-min steps; builds the URL via
  `helpers.url_for(t)`, derives `bucket`/`key`, **collapsing the doubled `//`** that
  `url_for` emits (STORE_PREFIX ends in `/`) so the key resolves on S3.
- Body shape matches the handler: `{"Records":[{"s3":{"bucket":{"name":BUCKET},
  "object":{"key":key}}}]}` (sent direct to SQS, not SNS-wrapped).
- Sends via `sqs.send_message_batch` (10/call — SQS max; independent of `SQS_BATCH_SIZE`,
  which is how many records the Lambda *pulls*). Thread pool of ~10–20 workers.
- **Resumable:** checkpoints the last-enqueued timestamp locally so a crash resumes without
  double-enqueueing (duplicates are harmless — region writes are idempotent and the rebase
  solver resolves concurrent ones — just wasteful).

Example (used for the month-scale test, 28 days in ~10 min):

```
python scripts/dispatch.py \
  --queue-url https://sqs.us-west-2.amazonaws.com/444055461661/gpmimerg-vz-dp-queue \
  --start 1998-03-25T23:30:00 --end 1999-03-26T00:00
```

## Verification

- **Unit (`tests/`):** `commit_processed_files` retries on a simulated `ConflictError` then
  succeeds after rebase (asserting the solver is `BasicConflictSolver`), and re-raises /
  propagates `RebaseFailedError` correctly; the credential provider and S3 registry are
  cached (constructed once); the dispatch message body has no `//` and round-trips through
  `_timestamp_from_url`.
- **Dry run (real S3):** dispatch one known granule, let the Lambda process it, then
  `xr.open_zarr` the store and assert the written slice is non-fill.
- **Post-build validation — `validate_build.ipynb`:** for a given time range,
  (1) **completeness** via per-chunk `store.exists(...)` — a metadata HEAD that directly
  answers whether each region write committed (preferred over a fill-value scan: the Zarr
  fill is an opaque per-variable sentinel, distinct from the CF `_FillValue` of -9999.9, so
  value-based detection is unreliable); and (2) a **matching spot-check** that samples random
  chunks *uniformly across the range* and compares store reads against the source HDF5 read
  natively with `h5py`. Run it in `us-west-2` (the data reads dereference GES DISC byte
  ranges, which require same-region access). Confirm DLQ is empty and redrive any stragglers
  (idempotent).
