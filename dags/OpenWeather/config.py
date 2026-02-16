import os
from dotenv import load_dotenv

load_dotenv()

OPENWEATHER_FORECAST_URL = os.getenv("OPENWEATHER_FORECAST_URL")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
OPENWEATHER_BASE_URL = os.getenv("OPENWEATHER_BASE_URL", "https://api.openweathermap.org/data/2.5/weather")

if not OPENWEATHER_API_KEY:
    raise RuntimeError("Missing OPENWEATHER_API_KEY (set it in your environment or .env)")
