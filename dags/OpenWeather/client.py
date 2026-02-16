import requests
from config import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL, OPENWEATHER_FORECAST_URL




def get_current_weather(city: str, country: str, units="metric"):
    
    params_weather = {
        "q": f"{city},{country}",
        "appid": OPENWEATHER_API_KEY,
        "units": units
    }
    
    response = requests.get(OPENWEATHER_BASE_URL, params=params_weather, timeout=15)
    response.raise_for_status()
    return response.json()

def get_weather_by_coords(lat: float, lon: float, units: str = "metric") -> dict:
    url = OPENWEATHER_BASE_URL
    params = {"lat": lat, "lon": lon, "appid": OPENWEATHER_API_KEY, "units": units}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()
