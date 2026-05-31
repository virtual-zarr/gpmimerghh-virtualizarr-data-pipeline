import json
from datetime import datetime

import dispatch
import pytest
from virtualizarr_processor import helpers
from virtualizarr_processor.processor import _time_index_for, _timestamp_from_url


def test_message_body_shape_and_clean_key() -> None:
    body = json.loads(dispatch.message_body(helpers.T0))
    s3 = body["Records"][0]["s3"]
    assert s3["bucket"]["name"] == helpers.BUCKET
    key = s3["object"]["key"]
    # url_for builds a clean key (no doubled slash) so it resolves on S3.
    assert "//" not in key
    assert key.startswith("GPM_L3/GPM_3IMERGHH.07/")


@pytest.mark.parametrize(
    "t, expected_idx",
    [
        (helpers.T0, 0),
        (datetime(1998, 1, 1, 0, 30), 1),
        (datetime(2025, 9, 30, 23, 30), helpers.N_TIME - 1),
    ],
)
def test_key_roundtrips_to_time_index(t: datetime, expected_idx: int) -> None:
    """The dispatched key must parse back to the same time index Stage 2 writes."""
    s3 = json.loads(dispatch.message_body(t))["Records"][0]["s3"]
    url = f"s3://{s3['bucket']['name']}/{s3['object']['key']}"
    assert _time_index_for(_timestamp_from_url(url)) == expected_idx


def test_full_range_enumerates_n_time() -> None:
    count = sum(1 for _ in dispatch._iter_timestamps(helpers.T0, helpers.T_MINUS_1))
    assert count == helpers.N_TIME


def test_send_batch_retries_partial_failures() -> None:
    """A partial-failure response retries only the failed entries, then succeeds."""
    calls = []

    class FakeSqs:
        def send_message_batch(self, QueueUrl: str, Entries: list[dict]) -> dict:
            calls.append([e["Id"] for e in Entries])
            if len(calls) == 1:
                return {"Failed": [{"Id": Entries[0]["Id"]}]}
            return {"Successful": [{"Id": e["Id"]} for e in Entries]}

    timestamps = [helpers.T0, datetime(1998, 1, 1, 0, 30)]
    dispatch._send_batch(FakeSqs(), "q", timestamps)

    assert calls[0] == ["0", "1"]
    assert calls[1] == ["0"]  # only the failed entry is retried


def test_send_batch_raises_when_failures_persist() -> None:
    class AlwaysFails:
        def send_message_batch(self, QueueUrl: str, Entries: list[dict]) -> dict:
            return {"Failed": [{"Id": e["Id"], "Code": "x"} for e in Entries]}

    with pytest.raises(RuntimeError):
        dispatch._send_batch(AlwaysFails(), "q", [helpers.T0], max_retries=3)
