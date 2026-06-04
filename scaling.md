# Scale the GPM IMERG HH virtual Icechunk build

This document records what we built to scale to 28 years of half-hourly data, why, and what we learned tuning it.

The pipeline, documented in `design.md`, writes virtual references for **486,480
granules** (files produced every 30 minutes from 1998-01-01 to 2025-10-01) into one Icechunk store.

## The core constraint

**Icechunk commits determine max throughput, not per-file processing.** An Icechunk commit is a
compare-and-swap on the head of the `main` branch, so commits can only happen serially. Only one
batch can be committed at a time.

There are 2 consequences of this:

- **Raising `MAX_CONCURRENCY` (the number of lambdas executing concurrently) does not raise throughput.** More Lambdas just queue at the commit point (and may error on the Earthdata auth endpoint, more on that below). The real throughput lever is **fewer, larger commits**, i.e. a bigger `SQS_BATCH_SIZE`.
- **Per-file processing is relatively cheap and network-bound**. Processing time is ~0.5 s/granule. Processing involves HDF5 header
  range-reads from the GES DISC Earthdata Cloud bucket. Memory consumption is also minimal, since we are reading byte-range metadata, not array data.

## Other scaling considerations:

The manifest is split **~1 year per shard** (`open_or_create_repo` uses
`manifest_split_size = 48 * 365 = 17,520`). This matters for scaling: each commit rewrites
the shard(s) its batch touched, so a shard's rewrite cost grows as the shard grows.

## What we changed (and why)

While scaling, the following changes were made:

### 1. Resolve rebase conflicts during commit — `Processor.commit_processed_files` (processor.py)

The original bare `session.commit()` meant any concurrent commit conflict failed the whole
batch, resulting in SQS redelivery.

2 changes were made to prevent SQS redeliver:

Frist, **the commit_processed_files includes a conflict solver that *resolves*, rather than just *detects* conflicts.** `ConflictDetector` always raises `RebaseFailedError`
("Snapshot cannot be rebased. Aborting rebase"). We instead use:

```python
session.rebase(icechunk.BasicConflictSolver(
    on_chunk_conflict=icechunk.VersionSelection.UseOurs
))
```

We don't expect to encounter a chunk conflict error except in a race condition. The pipeline is designed to only be writing disjoint slices. However, there is a chance that the same message for the same granule is being processed by 2 lambdas at once. This is because SQS guarantees deliver "at least once", so it could redeliver. Further, if messages could be redelivered if the SQS visibility timeout is less than the Lambda timeout (although this is prevented with a validation step in the CDK code, manual changes could happen). A race condition where the same granule is being processed by 2 concurrent lambdas could result in a `ChunkDoubleUpdate` error. The writes should be identical, so it is safe to use the `UseOurs` rule when this race condistion happens.

Second, a bounded, jittered-backoff retry loop will retry any  `ConflictError` errors. When lambdas are creating sessions and committing around the same time, it can cause an "Failed to commit, expected parent: XXX, actual parent YYY" type error. When this happens, the commit is retried up to `max_attempts` with backoff and jitter parameters. The first attempt happens quickly, and the wait between attempts grows exponentially up to `max_backoff`.

### 2. Cache the Earthdata credential provider — `helpers.py`

Request bursts intermittently fail with `UnauthenticatedError` (surfaced as a
wrapped `SystemError` crossing the Rust/Python boundary). We memoize both with
`@lru_cache(maxsize=1)`, so one provider + store are reused across the whole batch in a warm
container, hitting the endpoint roughly once per credential lifetime.

**Caveat that bounds concurrency:** the cache is *per process*. It does nothing across
containers, so the auth-endpoint load scales with `MAX_CONCURRENCY`, not with how many
granules each container reads.

### 3. Added more configurable scaling settings in `cdk/`

The following **settings were added to `cdk/settings.py`: `LAMBDA_TIMEOUT`, `LAMBDA_MEMORY`, `VISIBILITY_TIMEOUT`, and `SQS_MAX_BATCHING_WINDOW`. This is in addition to the exsiting tunable settings `MAX_CONCURRENCY`, `SQS_BATCH_SIZE` and `GARBAGE_COLLECTION_FREQUENCY`.

AWS **requires** a batching window ≥ 1 s once `SQS_BATCH_SIZE > 10`. The SQS Batching Window defines how long a lambda should wait to fill a batch (that is, receive `SQS_BATCH_SIZE` number of messages) before processing those messages as a batch. A `_check_batching_window` validate ensures `SQS_MAX_BATCHING_WINDOW` is set when `SQS_BATCH_SIZE > 10`.

## Operational lessons learned

- **`VISIBILITY_TIMEOUT ≥ LAMBDA_TIMEOUT`** Allowing the SQS
  visibility timeout to be less than the Lambda timeout causes in-flight messages to become
  visible again and be reprocessed *concurrently*. This results in a flood of duplicate same-slice writes.
  A settings validator guarantees `VISIBILITY_TIMEOUT ≥ LAMBDA_TIMEOUT` when deploying via CDK-managed.
  AWS best practice is ~6× the Lambda timeout for redrive headroom.
- **Keep `MAX_CONCURRENCY` moderate (~50).** At 50 we saw ~10 transient auth errors over a
  week of granules (self-healing). At **1000** the Earthdata auth endpoint was overwhelmed by ~1000
  simultaneous cold-container credential fetches → widespread `UnauthenticatedError`. Since
  concurrency buys no throughput (commits serialize) and *aggravates* both auth load and
  rebase churn, there's no reason to go high.
- **Batch size is the throughput lever, capped by `LAMBDA_TIMEOUT`.** Bigger batches mean
  fewer serialized commits *and* fewer credential fetches per granule (one cached token
  amortized over the batch). The cap: `batch_size × per_granule_time` must finish within the
  timeout with margin.

## Measured numbers

| Config (`MAX_CONCURRENCY` / `SQS_BATCH_SIZE`) | Lambda duration | Throughput | Notes |
|---|---|---|---|
| 50 / 25 | rough estimate of ~12 s/batch | 1 week (336 granules) processed in ~6 min | ~10 transient auth errors |
| 50 / 100 | under timeout | 1 week in ~2 min; 1 month (~1,440) in ~10 min | DLQ 0 |
| 50 / 100 | some timeouts | 5 years in ~7 hours | DLQ 1,131 |
| 25 / 100 | 99th percentile ~3 minutes; 50th percentile ~1 minute | 1 year | DLQ 0 |

At `SQS_BATCH_SIZE=100`, processing time is well under the 300 second timeout. So batch size can grow substantially **without** raising the timeout or memory (`LAMBDA_MEMORY=4096`).

Average memory use was ~2000MB, with max memory used 2609. So we could safely reduce memory to 3000 MB.

This was determined by running the following query in CloudWatch Log Insights for the processing lambdas CloudWatch Logs:


```sql
filter @type = "REPORT"
    | stats max(@memorySize / 1000 / 1000) as provisonedMemoryMB,
        min(@maxMemoryUsed / 1000 / 1000) as smallestMemoryRequestMB,
        avg(@maxMemoryUsed / 1000 / 1000) as avgMemoryUsedMB,
        max(@maxMemoryUsed / 1000 / 1000) as maxMemoryUsedMB,
        provisonedMemoryMB - maxMemoryUsedMB as overProvisionedMB
```

## Why performance at lower temporal ranges won't scale linearly

Tests at relatively small scale (say a week or month) do not saturate resources as currently configured. Two effects only appear at scale:

1. **Concurrency saturation.** When the number of batches is below `MAX_CONCURRENCY` there is idle capacity.
2. **Shard fill.** As the shard fills from 0 → 17,520 refs commit duration is expected to increase. In practice, some increased duration has been observed but it does not appear significant.

## Dispatch script — `scripts/dispatch.py`

The dispatch script enumerates files and dispatches messages to SQS. There is no reading from the files so it can be run from a local laptop not on AWS.

### What the dispatch script does
- Iterates over half-open `[--start, --end)` in 30-min steps; builds the URL via
  `helpers.url_for(t)`, derives `bucket`/`key`, **collapsing the doubled `//`** that
  `url_for` emits (STORE_PREFIX ends in `/`) so the key resolves on S3.
- Message body schema: `{"Records":[{"s3":{"bucket":{"name":BUCKET},
  "object":{"key":key}}}]}`.
- Sends via `sqs.send_message_batch`. It sends 10 per call, which is the SQS max and independent of `SQS_BATCH_SIZE`, using a thread pool of ~10–20 workers.
- **Resumable:** The dispatch script checkpoints the last-enqueued timestamp locally so a crash resumes without
  double-enqueueing (duplicates are harmless — region writes are idempotent and the rebase
  solver resolves concurrent ones — just wasteful).

Example (used for the month-scale test, 28 days in ~10 min):

```
python scripts/dispatch.py \
  --queue-url https://sqs.us-west-2.amazonaws.com/444055461661/gpmimerg-vz-dp-queue \
  --start 1998-03-25T23:30:00 --end 1999-03-26T00:00
```

## Post-build validation

A superficial way to validate all messages were processed is to check the dead-letter queue is empty. However, this isn't very reassuring on its own.

The `validate_build.ipynb` notebook provides a more thorough validation, albeit not exhaustive. For a given temporal range, it performs:

1. **a completeness check** via per-chunk `store.exists(...)` — a metadata HEAD that directly
  answers whether each region write committed, and,
1. **a spot-check** that samples random chunks uniformly across the range and compares store reads against the source HDF5 read
  natively with `h5py`. This section must be run in `us-west-2` as it will read from the GES DISC Earthdata Cloud bucket which require same-region access.

## Recovery

If validation turns up missing timesteps, it is safe to re-dispatch those timesteps as region writes are idempotent.

> [!Note]
> The completeness scan counts missing *chunks*, so one missing timestep shows up as **4 missing chunks**. The DLQ counts *messages* — one per granule
(= one timestep). So divide the chunk count by 4 before comparing.

Two ways to redrive:

- **SQS console DLQ redrive (TESTED)** (`Start DLQ redrive` / `StartMessageMoveTask`) — moves messages
  *physically in the DLQ* back to the source queue. Zero code, simplest. **Sufficient when
  missing-timesteps==DLQ count** (no silent loss). Limited to what's in the DLQ, and subject
  to DLQ retention (messages age out).
- **Notebook-driven redrive (UNTESTED)** (`validate_build.ipynb`, Section 3) — derives the missing set from
  `store.exists(...)`, dedupes to unique time indices, and re-sends one SQS message per missing
  timestep (same body as `dispatch.py`).

Rule of thumb: if missing-timesteps == DLQ count, use the console (easier); if missing > DLQ,
the queue isn't the source of truth — the store is — so redrive from the scan.
