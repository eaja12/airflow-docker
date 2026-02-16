from client import get_current_weather
from transforms import normalize_weather_response
from storage import write_to_duckdb, write_to_parquet_append


def main():
    raw = get_current_weather("Seattle", "US")
    df = normalize_weather_response(raw)

    write_to_duckdb(df)
    write_to_parquet_append(df)

    print("Stored 1 row.")
    print(df)

if __name__ == "__main__":
    main()