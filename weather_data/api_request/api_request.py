import os
import requests
from dotenv import load_dotenv
from requests.exceptions import RequestException
from typing import Optional, Dict, List


def get_api_key() -> str:
    """Load and return API key from the environment (.env supported)."""
    load_dotenv()
    key = os.getenv("API_KEY")
    if not key:
        raise ValueError("API_KEY not found in environment variables.")
    return key


def fetch_weather_data(city: str, base_url: str = "http://api.weatherstack.com/current", params: Optional[Dict] = None, headers: Optional[Dict] = None) -> Dict:
    """
    Fetch current weather data for a city using Weatherstack API.

    Args:
      city: City name to query (e.g., "Mumbai").
      base_url: Weather API endpoint.
      params: Extra query params to merge.
      headers: Optional HTTP headers.

    Returns:
      Parsed JSON dict from the API.
    """
    key = get_api_key()
    query_params = {"access_key": key, "query": city}
    if params:
        query_params.update(params)
    try:
        resp = requests.get(base_url, params=query_params, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except RequestException as e:
        raise RuntimeError(f"Failed to fetch weather data: {e}") from e


def mock_data(city: str = "Delhi") -> List[Dict]:
    """
    Lightweight helper returning a list with a single live API response.
    Kept for compatibility with existing code paths (e.g., insert_data.py).
    """
    data = fetch_weather_data(city)
    return [data]


 
