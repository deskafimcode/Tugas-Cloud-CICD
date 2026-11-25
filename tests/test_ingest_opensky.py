from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
import pandas as pd
from src.ingest_opensky import (
    build_daily_blob_path,
    fetch_opensky_global_states,
    merge_daily_dataframe,
    states_to_dataframe,
)

WIB = timezone(timedelta(hours=7))


def test_build_daily_blob_path():
    dt = datetime(2026, 11, 25, 6, 45, 0, tzinfo=WIB)
    path = build_daily_blob_path(dt)
    assert path == "opensky_global/year=2026/month=11/opensky_2026-11-25_WIB.csv"


def test_merge_daily_dataframe_deduplicates():
    dt = datetime(2026, 11, 25, 6, 0, 0, tzinfo=WIB)
    payload_1 = {
        "time": 1732500000,
        "states": [
            ["8A0123", "GIA123 ", "Indonesia", 1732499995, 1732499999, 110.82, -7.55, 9000.0, False, 200.0, 90.0, 0.0, None, 9200.0, "1200", False, 0],
            ["7C1111", "QFA456 ", "Australia", 1732499995, 1732499999, 151.20, -33.86, 10000.0, False, 230.0, 180.0, 0.0, None, 10200.0, "2000", False, 0],
        ],
    }
    payload_2 = {
        "time": 1732500000,
        "states": [
            ["8a0123", "GIA123 ", "Indonesia", 1732499995, 1732499999, 110.85, -7.54, 9050.0, False, 205.0, 92.0, 0.0, None, 9250.0, "1200", False, 0],
        ],
    }

    df1 = states_to_dataframe(payload_1, dt)
    df2 = states_to_dataframe(payload_2, dt)
    merged = merge_daily_dataframe(df1, df2)

    assert len(merged) == 2
    gia_row = merged[merged["icao24"] == "8a0123"].iloc[0]
    assert gia_row["baro_altitude"] == 9050.0


@patch("src.ingest_opensky.requests.get")
def test_fetch_opensky_global_states(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"time": 1732500000, "states": []}
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    res = fetch_opensky_global_states()
    assert res["time"] == 1732500000