"""Lambda (extraction): S3 landing CSV -> Kinesis events, one chunk per invocation.

Step Functions calls it in a loop, feeding the returned event back in until "done" is true:

    in : {"bucket": "lakehouse-landing", "key": "anp/file.csv", "offset": 0, "limit": 1000,
          "fault_rate": 0.05, "seed": 42, "sep": ";", "encoding": "utf-8-sig"}
    out: the same event with "offset" advanced, plus "sent" and "done".

Each call re-reads the file from the start and skips `offset` rows (streaming, low memory).
TODO: keep a byte offset instead, for very large files.
"""
import csv
import os
import random
from itertools import islice

from .aws import client
from .events import events_for_row, send

STREAM = os.getenv("KINESIS_STREAM", "anp-events")


def build_events(lines, sep, offset, limit, fault_rate, seed):
    """Pure function: CSV lines -> (events, rows_read) for the chunk [offset, offset + limit)."""
    reader = csv.DictReader(lines, delimiter=sep)
    rows = list(islice(reader, offset, offset + limit))
    rng = random.Random(f"{seed}:{offset}")  # deterministic per chunk, so retries repeat the same faults
    events = [e for row in rows for e in events_for_row(row, fault_rate, rng)]
    return events, len(rows)


def handler(event, context=None):
    offset = int(event.get("offset", 0))
    limit = int(event.get("limit", 1000))
    encoding = event.get("encoding", "utf-8-sig")

    s3 = client("s3")
    body = s3.get_object(Bucket=event["bucket"], Key=event["key"])["Body"]
    lines = (raw.decode(encoding) for raw in body.iter_lines())

    events, rows_read = build_events(
        lines,
        event.get("sep", ";"),
        offset,
        limit,
        float(event.get("fault_rate", 0.05)),
        event.get("seed", 42),
    )
    send(client("kinesis"), event.get("stream", STREAM), events)
    return {**event, "offset": offset + rows_read, "sent": len(events), "done": rows_read < limit}
