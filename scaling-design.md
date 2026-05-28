# Scale the GPM IMERG HH virtual Icechunk build

## Context

The pipeline (3-stage SQS + Lambda, design.md) must write virtual references for
**486,480 granules** into one Icechunk store. Scaling to ~half a million messages exposes two issues:

1. **Commits serialize on `main` and aren't rebase-safe.** `Processor.commit_processed_files`
   (`lambda/virtualizarr-processor/virtualizarr_processor/processor.py:259-261`) does a
   bare `session.commit()`. Icechunk commits are a compare-and-swap on the branch tip, so
   only one batch can land at a time. With `MAX_CONCURRENCY=50`, most concurrent commits
   conflict; the handler then marks the **whole batch** failed (`lambda/process_messages/handler.py:110-116`),
   SQS redelivers, and after `max_receive_count=20` the messages hit the DLQ. Because every
   granule writes a **disjoint** time slice, a rebase resolves cleanly — we just never call it.
2. **The main queue has no retention period** (`cdk/stack.py:70-79`), so it defaults to
   **4 days**. A 486k-message backfill that runs longer than 4 days will silently drop
   unprocessed messages.

Key insight that drives tuning: **commit throughput, not per-file processing, is the
ceiling.** Raising concurrency does not raise throughput — it multiplies rebase churn.
The levers that help are *fewer, larger commits* (bigger `SQS_BATCH_SIZE`) and a
*rebase-retry* loop on commit, with concurrency kept moderate.

- Add a rebase-retry loop
- Add dispatch script
- Add a tuning runbook.

## Existing pieces to reuse

- `helpers.url_for(t)` (`helpers.py:54-67`) — deterministic timestamp → full `s3://` URL.
- `helpers.T0`, `helpers.T_MINUS_1`, `helpers.N_TIME`, `helpers.BUCKET` (`helpers.py:42-51`).
- `Processor._time_index_for` / `_timestamp_from_url` (`processor.py:49-68`) — already prove
  the filename ↔ time-index mapping; the dispatch script does not need a time index, only the URL.
- Message shape expected by the handler: `process_notification` reads
  `message["Records"][0]["s3"]["bucket"]["name"]` and `["object"]["key"]` (`handler.py:34-35`).

## Dispatch script (runs from laptop)

New file: `scripts/dispatch.py` (add a `scripts/` dir). Pure enumeration + SQS sends; no
HDF5 reads, no AWS compute. Runs locally with `sqs:SendMessage` perms and the queue URL.

Behavior:
- Iterate `t = T0 → T_MINUS_1` step 30 min (use `helpers.N_TIME` as the count check).
- For each `t`, build the URL via `helpers.url_for(t)`, then derive `bucket`/`key`.
  **Important:** `url_for` emits a doubled slash (STORE_PREFIX ends in `/`). Build the key
  as everything after `s3://{BUCKET}/` and **collapse the `//`** so it resolves to the real
  S3 object. (The single-file dry run confirms the key is correct — see Verification.)
- Build body `{"Records": [{"s3": {"bucket": {"name": BUCKET}, "object": {"key": key}}}]}`
  (matches `handler.py:34-35`; send to SQS directly, not SNS-wrapped).
- Send via `sqs.send_message_batch` (10 entries/call — SQS max; this is the *send* batch and
  is independent of `SQS_BATCH_SIZE`, which is how many records the Lambda *pulls*).
- Use a thread pool (~10–20 workers) for the ~48,648 batch calls — minutes from a laptop.
- **Resumable:** checkpoint the last-enqueued timestamp to a local file so a mid-run crash can
  resume without double-enqueueing (region writes are idempotent, so duplicates are harmless
  but wasteful).
- Flags: `--start`, `--end` (default T0/T_MINUS_1) to scope partial runs (e.g. one year for the
  year-scale test), `--dry-run` (print counts, send nothing), `--queue-url`.

## Rebase-retry commit (processor.py)

Edit `Processor.commit_processed_files` (`processor.py:259-261`) to loop:

```
for attempt in range(max_attempts):          # e.g. 10
    try:
        return str(session.commit(message=...))
    except icechunk.ConflictError:
        session.rebase(icechunk.ConflictDetector())   # disjoint slices => resolves clean
        sleep(backoff with jitter)
# exhausted -> raise so the handler fails the batch and SQS redelivers
```

Use icechunk's default/basic conflict solver (`ConflictDetector` — no `OnChunkConflict`
override needed since slices never overlap). Bounded attempts + jittered exponential backoff
so a thundering herd spreads out. Exhaustion re-raises → existing handler path
(`handler.py:110-116`) marks the batch failed and SQS redelivers, which is the correct fallback.
This makes the common case succeed in-Lambda instead of via redelivery, which is what keeps
messages out of the DLQ.

## Infrastructure hardening + tunable knobs (cdk)

- **Fix queue retention** (`cdk/stack.py:70-79`): add `retention_period=Duration.days(14)` to
  the main queue. Without this a multi-day backfill loses messages.
- **Parameterize the knobs** so tuning = redeploy with env vars, no code edits. In
  `cdk/settings.py` add (with defaults): `LAMBDA_TIMEOUT` (sec, default 300; raise toward 900
  to allow bigger batches), `LAMBDA_MEMORY` (MB, default 2048), `VISIBILITY_TIMEOUT`
  (sec, default 1800). Wire them into `stack.py:122-123` (timeout/memory) and `stack.py:74`
  (visibility). Keep the invariant **visibility_timeout ≥ lambda_timeout** (add an assert in
  settings). `SQS_BATCH_SIZE` and `MAX_CONCURRENCY` already exist (`settings.py:28-29`,
  used at `stack.py:153-155`).

## Test sequence (day, week, month, year, five-years, full)

Run the staged sequence from design.md, measuring at each step. Concrete starting points and
what to watch:

1. **Single-file dry run** dispatch one granule, confirm the key resolves
   (no `//` 404), one region write lands, `xr.open_zarr` reads it.
2. **Year-scale (1998, 17,520 msgs).** Start `SQS_BATCH_SIZE=50`, `MAX_CONCURRENCY=10`,
   `LAMBDA_TIMEOUT=900`. Measure from CloudWatch: commit duration, conflict/retry counts,
   Lambda wall-time per batch, DLQ depth (should stay 0), and the resulting per-shard manifest
   size (target ~80–200 MB/shard as in design.md).
   - Per-file time is network-bound (HDF5 header range-reads from GES DISC). Confirm
     `batch_size × per_file_time` stays well under `LAMBDA_TIMEOUT` with margin.
   - If commits are the bottleneck (Lambdas idle waiting to commit): **raise `SQS_BATCH_SIZE`**
     (fewer, larger commits), not concurrency. Bigger batches also reduce manifest-shard
     rewrite amplification (each shard is re-written fewer times as it fills).
   - If you see steady conflicts/retries even with the rebase loop: **lower `MAX_CONCURRENCY`**.
3. **Concurrency stress (5 years).** Confirm DLQ stays empty and commit latency is stable as the
   active manifest shard grows.
4. **Full build (486k).** Apply tuned values. Watch DLQ; redrive any stragglers (idempotent).

Note on an advanced lever (only if single-branch commit throughput proves unacceptable after
tuning): Icechunk's distributed `Session.fork`/merge pattern commits many region writes in one
shot, but it needs a coordinator and does **not** fit the per-batch-Lambda VDP model — it'd be a
separate execution path. Out of scope unless the runbook shows we need it.

## Verification

- **Unit:** extend `tests/` — a test that `commit_processed_files` retries on a simulated
  `ConflictError` then succeeds after `rebase`; a dispatch-script test asserting the message body
  shape and that the derived key has no `//` and round-trips through `_timestamp_from_url`.
- **Dry run (real S3):** dispatch a single known granule, let the Lambda process it, then
  `xr.open_zarr` the store and assert the written time slice is non-fill — this validates the
  key resolves on S3 and the end-to-end path.
- **Post-build validation (design.md):** scan for timesteps whose mean equals the Zarr fill value
  (indicates a missed write); confirm DLQ is empty; spot-check random chunks against source byte
ranges.

python scripts/dispatch.py --queue-url https://sqs.us-west-2.amazonaws.com/444055461661/gpmimerg-vz-dp-queue \
  --start 1998-01-01T00:30 --end 1998-01-07T00:00
