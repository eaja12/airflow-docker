import pandas as pd
import os
from pathlib import Path

def write_to_csv():
    file_path = "/opt/airflow/files/days.csv"

    new_row = pd.DataFrame({"run_timestamp": [pd.Timestamp.now(tz="UTC")]})

    # If file doesn't exist OR is empty, write header
    write_header = (not os.path.exists(file_path)) or (os.path.getsize(file_path) == 0)

    new_row.to_csv(file_path, mode="a", header=write_header, index=False)
    print(f"Timestamp written to {file_path}")




def max_write_time(
    input_csv: str = "/opt/airflow/files/days.csv",
    output_csv: str = "/opt/airflow/files/max_timestamp.csv",
    timestamp_col: str = "run_timestamp",
) -> None:
    """
    Reads `input_csv`, finds the max value in `timestamp_col`,
    and overwrites `output_csv` with a single-row CSV containing that max.
    """
    input_path = Path(input_csv)
    output_path = Path(output_csv)

    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    df = pd.read_csv(input_path)

    if timestamp_col not in df.columns:
        raise ValueError(f"Column '{timestamp_col}' not found. Columns: {list(df.columns)}")

    # Parse timestamps robustly (handles strings, timezone, etc.)
    ts = pd.to_datetime(df[timestamp_col], errors="coerce", utc=True)

    # Drop unparseable rows
    ts = ts.dropna()
    if ts.empty:
        raise ValueError(f"No valid timestamps found in column '{timestamp_col}'")

    max_ts = ts.max()

    # Overwrite output with only the latest max
    out_df = pd.DataFrame(
        {
            "max_run_timestamp": [max_ts.isoformat()],
            "source_file": [str(input_path)],
            "computed_at_utc": [pd.Timestamp.now(tz="UTC").isoformat()],
        }
    )

    out_df.to_csv(output_path, index=False)
    print(f"Wrote max timestamp {max_ts.isoformat()} to {output_path}")
