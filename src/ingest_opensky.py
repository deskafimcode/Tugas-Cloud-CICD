import io
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
import pandas as pd
import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

OPENSKY_STATES_URL = "https://opensky-network.org/api/states/all"
WIB = timezone(timedelta(hours=7))

STATE_VECTOR_COLUMNS: List[str] = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]


def fetch_opensky_global_states(timeout: int = 30) -> Dict[str, Any]:
    """Mengambil data state vectors pesawat seluruh dunia dari OpenSky API."""
    response = requests.get(OPENSKY_STATES_URL, timeout=timeout)
    response.raise_for_status()
    return response.json()


def states_to_dataframe(payload: Dict[str, Any], ingested_wib: datetime) -> pd.DataFrame:
    """Mengonversi JSON OpenSky global menjadi DataFrame."""
    raw_states = payload.get("states") or []
    snapshot_unix = payload.get("time")

    df = pd.DataFrame(raw_states, columns=STATE_VECTOR_COLUMNS)
    if not df.empty:
        df["icao24"] = df["icao24"].fillna("").astype(str).str.strip().str.lower()
        df["callsign"] = df["callsign"].fillna("").astype(str).str.strip()

    df["snapshot_unix"] = snapshot_unix
    df["ingested_at_wib"] = ingested_wib.isoformat()
    df["wib_date"] = ingested_wib.strftime("%Y-%m-%d")
    df["wib_batch_hour"] = ingested_wib.strftime("%H:00")
    return df


def merge_daily_dataframe(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """Menggabungkan data harian dan menghapus duplikat per pesawat + snapshot."""
    if existing_df.empty:
        combined = new_df.copy()
    elif new_df.empty:
        combined = existing_df.copy()
    else:
        combined = pd.concat([existing_df, new_df], ignore_index=True)

    if not combined.empty:
        combined = combined.drop_duplicates(
            subset=["icao24", "snapshot_unix"], keep="last"
        ).reset_index(drop=True)
    return combined


def build_daily_blob_path(ingested_wib: datetime, prefix: str = "opensky_global") -> str:
    """Membentuk path 1 file CSV per hari WIB."""
    return (
        f"{prefix}/"
        f"year={ingested_wib.strftime('%Y')}/"
        f"month={ingested_wib.strftime('%m')}/"
        f"opensky_{ingested_wib.strftime('%Y-%m-%d')}_WIB.csv"
    )


def append_to_daily_csv_blob(
    new_df: pd.DataFrame,
    connection_string: str,
    container_name: str,
    ingested_wib: datetime,
    prefix: str = "opensky_global",
) -> tuple[str, int]:
    """Mengambil CSV harian jika sudah ada, menggabungkannya, lalu mengunggah ulang."""
    blob_path = build_daily_blob_path(ingested_wib=ingested_wib, prefix=prefix)
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_path)

    try:
        existing_bytes = blob_client.download_blob().readall()
        existing_df = pd.read_csv(io.BytesIO(existing_bytes), dtype={"icao24": str})
    except ResourceNotFoundError:
        existing_df = pd.DataFrame(columns=list(new_df.columns))

    merged_df = merge_daily_dataframe(existing_df=existing_df, new_df=new_df)

    csv_buffer = io.StringIO()
    merged_df.to_csv(csv_buffer, index=False)
    blob_client.upload_blob(
        csv_buffer.getvalue().encode("utf-8"),
        overwrite=True,
    )
    return blob_path, len(merged_df)


def run_pipeline() -> str:
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    container_name = os.getenv("AZURE_CONTAINER_NAME", "opensky-data")
    ingested_wib = datetime.now(WIB)

    payload = fetch_opensky_global_states()
    new_df = states_to_dataframe(payload, ingested_wib)
    blob_path, total_rows = append_to_daily_csv_blob(
        new_df=new_df,
        connection_string=conn_str,
        container_name=container_name,
        ingested_wib=ingested_wib,
    )
    print(
        f"Batch {ingested_wib.strftime('%Y-%m-%d %H:%M WIB')} selesai. "
        f"File: {blob_path} | Total baris harian: {total_rows}"
    )
    return blob_path


if __name__ == "__main__":
    run_pipeline()