from __future__ import annotations
import os
import duckdb
import pandas as pd
from datetime import datetime, timezone

DEFAULT_DB_PATH = os.path.join("data", "weather.duckdb")
DEFAULT_PARQUET_PATH = os.path.join("data", "weather.parquet")

def ensure_data_dir() -> None:
    os.makedirs("data", exist_ok=True)

def write_to_duckdb(df: pd.DataFrame, table: str = "weather_observations", db_path: str = DEFAULT_DB_PATH) -> None:
    ensure_data_dir()
    con = duckdb.connect(db_path)

    # Create table if not exists based on df schema
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")

    # Append rows
    con.register("df_view", df)
    con.execute(f"INSERT INTO {table} SELECT * FROM df_view")
    con.close()

def write_to_parquet_append(df: pd.DataFrame, parquet_path: str = DEFAULT_PARQUET_PATH) -> None:
    """
    Simple append pattern:
    - If file exists: read existing + concat + write back (fine for small projects)
    - For larger volumes: partitioned parquet by date is better (we can do that next).
    """
    ensure_data_dir()

    if os.path.exists(parquet_path):
        existing = duckdb.query(f"SELECT * FROM read_parquet('{parquet_path}')").to_df()
        df_out = pd.concat([existing, df], ignore_index=True)
    else:
        df_out = df

    # Use duckdb to write parquet reliably
    duckdb.register("df_out", df_out)
    duckdb.query(f"COPY df_out TO '{parquet_path}' (FORMAT PARQUET)")

def forecast_to_df(data: dict) -> pd.DataFrame:
    df = pd.json_normalize(data["list"])

    # Convert timestamp
    df["forecast_dt"] = pd.to_datetime(df["dt"], unit="s", utc=True)

    # Add city metadata
    df["city"] = data["city"]["name"]
    df["country"] = data["city"]["country"]
    df["lat"] = data["city"]["coord"]["lat"]
    df["lon"] = data["city"]["coord"]["lon"]

    return df



def weather_to_df(data: dict) -> pd.DataFrame:
    return pd.DataFrame([{
        "city": data["name"],
        "timestamp_utc": datetime.fromtimestamp(data["dt"], tz=timezone.utc),
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather_main": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "wind_speed": data["wind"]["speed"]
    }])
