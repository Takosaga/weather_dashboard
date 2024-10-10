from pathlib import Path
import typer
from loguru import logger
from tqdm import tqdm
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

from weather_dashboard.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()

def fetch_weather_data(latitude, longitude, city):
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m"
    }
    response = openmeteo.weather_api(url, params=params)[0]

    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "temperature_2m": hourly_temperature_2m,
        "city": city
    }

    return pd.DataFrame(data=hourly_data)

@app.command()
def main(
    output_path: Path = RAW_DATA_DIR / "baltic_capitals_weather.csv",
):
    logger.info("Fetching and processing weather data for Baltic capitals...")

    # Create directories if they don't exist
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    cities = [
        {"name": "Tallinn", "lat": 59.4370, "lon": 24.7536},
        {"name": "Riga", "lat": 56.9496, "lon": 24.1052},
        {"name": "Vilnius", "lat": 54.6872, "lon": 25.2797}
    ]

    all_data = []

    for city in tqdm(cities, desc="Processing cities"):
        logger.info(f"Fetching data for {city['name']}...")
        city_data = fetch_weather_data(city['lat'], city['lon'], city['name'])
        all_data.append(city_data)
        logger.info(f"Data fetched for {city['name']}.")

    combined_data = pd.concat(all_data, ignore_index=True)
    
    # Ensure the parent directory of the output file exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    combined_data.to_csv(output_path, index=False)
    
    logger.success(f"Processing complete. Data saved to {output_path}")

if __name__ == "__main__":
    app()