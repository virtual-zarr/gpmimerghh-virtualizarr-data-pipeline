#!/usr/bin/env python3
"""Dispatch one SQS message per GPM IMERG HH granule.

Enumerates GPM IMERG granules and sends to SQS. Safe to
run from a laptop with ``sqs:SendMessage`` on the target queue. The granule URLs
are deterministic (``helpers.url_for``), so no bucket listing is needed.

The run is resumable: after each wave of batches commits successfully, the last
enqueued timestamp is written to a checkpoint file. A re-run skips everything at
or before that timestamp. Region writes are idempotent, so the worst case of a
crash mid-wave is a few duplicate messages, not corruption.

Examples
--------
    # full dataset
    python scripts/dispatch.py --queue-url https://sqs.us-west-2.amazonaws.com/123/vdp-queue

    # one year, for the year-scale tuning run
    python scripts/dispatch.py --queue-url ... --start 1998-01-01 --end 1999-01-01

    # see what would be sent without sending anything
    python scripts/dispatch.py --start 1998-01-01 --end 1999-01-01 --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator

import boto3
from mypy_boto3_sqs.client import SQSClient
from virtualizarr_processor import helpers

STEP = timedelta(minutes=30)
SQS_MAX_BATCH = 10  # SQS send_message_batch hard limit
_MULTISLASH = re.compile(r"/{2,}")


def _iter_timestamps(start: datetime, end: datetime) -> Iterator[datetime]:
    """Half-open [start, end) in 30-minute steps."""
    t = start
    while t < end:
        yield t
        t += STEP


def message_body(t: datetime) -> str:
    """SQS body in the shape ``process_notification`` expects (handler.py).

    ``helpers.url_for`` emits a doubled slash (STORE_PREFIX ends in ``/``); we
    collapse it so the key resolves to the real S3 object.
    """
    url = helpers.url_for(t)
    prefix = f"s3://{helpers.BUCKET}/"
    if not url.startswith(prefix):
        raise ValueError(f"unexpected url for {t!r}: {url}")
    key = _MULTISLASH.sub("/", url[len(prefix) :])
    return json.dumps(
        {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": helpers.BUCKET},
                        "object": {"key": key},
                    }
                }
            ]
        }
    )


def _chunked(it: Iterable, n: int) -> Iterator[list]:
    chunk: list = []
    for x in it:
        chunk.append(x)
        if len(chunk) == n:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _send_batch(
    sqs: SQSClient, queue_url: str, timestamps: list[datetime], max_retries: int = 4
) -> None:
    """Send up to 10 messages, retrying any partial failures. Raises on give-up."""
    pending = [
        {"Id": str(i), "MessageBody": message_body(t)} for i, t in enumerate(timestamps)
    ]
    last_failed: list = []
    for _ in range(max_retries):
        resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=pending)
        last_failed = resp.get("Failed", [])
        if not last_failed:
            return
        failed_ids = {f["Id"] for f in last_failed}
        pending = [e for e in pending if e["Id"] in failed_ids]
    raise RuntimeError(
        f"{len(pending)} messages still failing after {max_retries} tries: "
        f"{last_failed[:3]}"
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--queue-url", help="Target SQS queue URL (required unless --dry-run)"
    )
    p.add_argument(
        "--start",
        type=datetime.fromisoformat,
        default=helpers.T0,
        help=f"ISO start, inclusive (default {helpers.T0.isoformat()})",
    )
    p.add_argument(
        "--end",
        type=datetime.fromisoformat,
        default=helpers.T_MINUS_1,
        help=f"ISO end, exclusive (default {helpers.T_MINUS_1.isoformat()})",
    )
    p.add_argument("--region", default="us-west-2", help="AWS region of the queue")
    p.add_argument("--workers", type=int, default=12, help="Concurrent send threads")
    p.add_argument(
        "--wave-batches",
        type=int,
        default=50,
        help="Batches per checkpoint wave (each batch is up to 10 messages)",
    )
    p.add_argument(
        "--checkpoint", default=".dispatch_checkpoint", help="Resume checkpoint file"
    )
    p.add_argument(
        "--no-resume", action="store_true", help="Ignore any existing checkpoint"
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Count + show a sample; send nothing"
    )
    args = p.parse_args(argv)
    if args.end <= args.start:
        p.error("--end must be after --start")
    if not args.dry_run and not args.queue_url:
        p.error("--queue-url is required unless --dry-run")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    checkpoint = Path(args.checkpoint)
    resume_after: datetime | None = None
    if checkpoint.exists() and not args.no_resume and not args.dry_run:
        resume_after = datetime.fromisoformat(checkpoint.read_text().strip())
        print(f"Resuming after checkpoint {resume_after.isoformat()}")

    def timestamps() -> Iterator[datetime]:
        for t in _iter_timestamps(args.start, args.end):
            if resume_after is None or t > resume_after:
                yield t

    if args.dry_run:
        count = sum(1 for _ in timestamps())
        sample = message_body(args.start)
        print(
            f"[dry-run] {count} messages from {args.start.isoformat()} "
            f"to {args.end.isoformat()}"
        )
        if args.start == helpers.T0 and args.end == helpers.T_MINUS_1:
            print(
                f"[dry-run] full-range expected N_TIME = {helpers.N_TIME} "
                f"(match: {count == helpers.N_TIME})"
            )
        print(f"[dry-run] sample body: {sample}")
        return 0

    sqs = boto3.client("sqs", region_name=args.region)
    sent = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for wave in _chunked(_chunked(timestamps(), SQS_MAX_BATCH), args.wave_batches):
            futures = [
                pool.submit(_send_batch, sqs, args.queue_url, batch) for batch in wave
            ]
            for f in futures:
                # propagate the first give-up; checkpoint stays at the last good wave
                f.result()
            sent += sum(len(b) for b in wave)
            wave_last = wave[-1][-1]
            checkpoint.write_text(wave_last.isoformat())
            print(f"sent {sent} (through {wave_last.isoformat()})", flush=True)

    print(f"Done. Enqueued {sent} messages.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
