"""
Integration tests that hit real AWS resources.

Requirements:
  - AWS credentials with read access to the source S3 bucket
  - ICECHUNK_BUCKET and ICECHUNK_PREFIX env vars pointing to a writable bucket

Run with:
  <AWS_CREDS_SET> uv run pytest -m integration

Local vs cloud behaviour
------------------------
Set CLOUD_INTEGRATION=true when running with in us-west-2 with access to
gesdisc-cumulus-prod-protected.  Without it the test is marked xfail —
it is expected to fail with a 403 / PermissionDenied error, and that failure
is treated as a pass so CI does not block on missing creds.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from aws_lambda_powertools.utilities.batch.exceptions import BatchProcessingError
from obstore.exceptions import PermissionDeniedError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "lambda"))

from process_messages.handler import handler

CLOUD_INTEGRATION = os.environ.get("CLOUD_INTEGRATION", "false").lower() == "true"


@pytest.mark.integration
def test_handler_processes_real_s3_file() -> None:
    event = {
        "Records": [
            {
                "messageId": "msg-000",
                "receiptHandle": "receipt-0",
                "body": (
                    '{"Records": [{"s3": {"bucket": {"name": "gesdisc-cumulus-prod-protected"},'  # noqa: E501
                    ' "object": {"key": "GPM_L3/GPM_3IMERGHH.07/2025/273/'
                    '3B-HHR.MS.MRG.3IMERG.20250930-S233000-E235959.1410.V07B.HDF5"}}}]}'
                ),
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1717600000000",
                    "ApproximateFirstReceiveTimestamp": "1717600000000",
                },
                "messageAttributes": {},
                "md5OfBody": "abc",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                "awsRegion": "us-east-1",
            }
        ]
    }

    context = MagicMock()
    context.function_name = "test-function"
    context.function_version = "$LATEST"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789:function:test"
    context.memory_limit_in_mb = 128
    context.aws_request_id = "test-request-id"
    context.log_group_name = "/aws/lambda/test-function"
    context.log_stream_name = "2024/01/01/[$LATEST]test"

    if CLOUD_INTEGRATION:
        response = handler(event, context)
        assert response["batchItemFailures"] == []
    else:
        # Without cloud credentials the handler runs until it hits
        # gesdisc-cumulus-prod-protected.
        # PermissionDeniedError surfaces directly if initialize_repo()
        # needs a sample file, or as BatchProcessingError if the repo is
        # already initialized and process_file() fails.
        with pytest.raises((PermissionDeniedError, BatchProcessingError)):
            handler(event, context)
