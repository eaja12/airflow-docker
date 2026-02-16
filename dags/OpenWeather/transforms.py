import pandas as pd
from datetime import datetime

def normalize_weather_response(data: dict) -> pd.DataFrame:
    record = {
        "city": data["name"],
        "timestamp_utc": datetime.utcfromtimestamp(data["dt"]),
        "temperature": data["main"]["temp"],
        "feels_like": data["main"]["feels_like"],
        "humidity": data["main"]["humidity"],
        "pressure": data["main"]["pressure"],
        "weather_main": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "wind_speed": data["wind"]["speed"],
    }
    return pd.DataFrame([record])
