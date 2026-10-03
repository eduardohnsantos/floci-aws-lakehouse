from lakehouse.aggregate import aggregate_fields, dedupe_latest, normalize_period


def _rec(well, period, oil, ingested_at, field="CAMPO A"):
    return {
        "event_id": f"{well}-{period}-{ingested_at}",
        "ingested_at": ingested_at,
        "basin": "Santos",
        "field": field,
        "well": well,
        "period": period,
        "oil_volume": oil,
    }


def test_normalize_period():
    assert normalize_period("2021/05") == "2021-05"
    assert normalize_period("05/2021") == "2021-05"
    assert normalize_period("2021-05") == "2021-05"


def test_dedupe_keeps_latest_ingestion_per_well_and_period():
    records = [
        _rec("W1", "2021/05", 10.0, "2026-10-02T02:00:00"),
        _rec("W1", "2021/05", 12.0, "2026-10-02T03:00:00"),  # same well/period, newer
        _rec("W2", "2021/05", 5.0, "2026-10-02T02:00:00"),
    ]
    wells = dedupe_latest(records)
    assert [w["well"] for w in wells] == ["W1", "W2"]
    assert wells[0]["oil_volume"] == 12.0
    assert wells[0]["period"] == "2021-05"


def test_same_well_in_different_periods_is_kept():
    records = [
        _rec("W1", "2021/05", 10.0, "2026-10-02T02:00:00"),
        _rec("W1", "2021/06", 11.0, "2026-10-02T02:00:00"),
    ]
    assert len(dedupe_latest(records)) == 2


def test_aggregate_fields_totals_and_average():
    wells = dedupe_latest(
        [
            _rec("W1", "2021/05", 10.0, "t1"),
            _rec("W2", "2021/05", 20.0, "t1"),
            _rec("W3", "2021/05", 7.0, "t1", field="CAMPO B"),
        ]
    )
    rows = {r["field"]: r for r in aggregate_fields(wells)}
    assert rows["CAMPO A"]["total_oil"] == 30.0
    assert rows["CAMPO A"]["wells"] == 2
    assert rows["CAMPO A"]["avg_oil_per_well"] == 15.0
    assert rows["CAMPO B"]["wells"] == 1


def test_missing_field_goes_to_unknown():
    wells = dedupe_latest([{**_rec("W1", "2021/05", 1.0, "t1"), "field": None}])
    assert aggregate_fields(wells)[0]["field"] == "UNKNOWN"
