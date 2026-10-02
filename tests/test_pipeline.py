import json
import random

from lakehouse.handler import process_lines
from lakehouse.events import events_for_row
from lakehouse.schema import canonicalize, normalize_key, to_float
from lakehouse.validation import validate


def test_normalize_key():
    assert normalize_key("Óleo (bbl/dia)") == "oleo_bbl_dia"
    assert normalize_key("Mês/Ano") == "mes_ano"


def test_to_float_brazilian_formats():
    assert to_float("1.234,50") == 1234.5
    assert to_float("74,4653") == 74.4653
    assert to_float("") is None


def test_valid_row():
    row = canonicalize({"Poço": "7-MLL-77D-RJS", "Período": "2021/05", "Óleo (bbl/dia)": "74,4653"})
    assert row["well"] == "7-MLL-77D-RJS"
    assert validate(row) == []


def test_validation_errors():
    assert "missing_well" in validate({"period": "2021/05", "oil_volume": "1"})
    assert "invalid_oil_volume" in validate({"well": "w", "period": "2021/05", "oil_volume": "n/a"})
    assert "negative_oil_volume" in validate({"well": "w", "period": "2021/05", "oil_volume": "-1"})
    assert "invalid_period" in validate({"well": "w", "period": "maio", "oil_volume": "1"})


def _line(event_id, raw):
    return json.dumps({"event_id": event_id, "ingested_at": "2026-01-01T00:00:00+00:00", "raw": raw})


def test_process_lines_splits_valid_rejected_and_dedups():
    good = {"Poço": "W1", "Período": "2021/05", "Óleo (bbl/dia)": "10"}
    bad = {"Poço": "", "Período": "2021/05", "Óleo (bbl/dia)": "10"}
    valid, rejected, duplicates = process_lines(
        [_line("a", good), _line("a", good), _line("b", bad), "not json"]
    )
    assert len(valid) == 1
    assert duplicates == 1
    assert len(rejected) == 2


def test_producer_without_faults_sends_one_clean_event():
    row = {"Poço": "W1", "Período": "2021/05", "Óleo (bbl/dia)": "10"}
    events = events_for_row(row, 0.0, random.Random(1))
    assert len(events) == 1
    assert events[0]["raw"] == row


def test_injected_faults_are_caught_by_validation():
    row = {"Poço": "W1", "Período": "2021/05", "Óleo (bbl/dia)": "10"}
    rng = random.Random(7)
    caught = 0
    for _ in range(200):
        for event in events_for_row(row, 1.0, rng):
            if validate(canonicalize(event["raw"])):
                caught += 1
    assert caught > 0
