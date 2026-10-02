from pathlib import Path

from lakehouse.extract import build_events

SAMPLE = Path(__file__).parent.parent / "data" / "sample" / "anp_sample.csv"


def _lines():
    return SAMPLE.read_text(encoding="utf-8").splitlines()


def test_first_chunk_is_not_done():
    events, rows_read = build_events(_lines(), ";", 0, 2, 0.0, 42)
    assert rows_read == 2
    assert len(events) == 2


def test_last_chunk_reads_the_remaining_rows():
    events, rows_read = build_events(_lines(), ";", 2, 10, 0.0, 42)
    assert rows_read == 3  # sample has 5 data rows
    assert len(events) == 3


def test_chunks_are_deterministic_for_retries():
    a, _ = build_events(_lines(), ";", 0, 5, 1.0, 7)
    b, _ = build_events(_lines(), ";", 0, 5, 1.0, 7)
    assert [e["event_id"] for e in a] == [e["event_id"] for e in b]
