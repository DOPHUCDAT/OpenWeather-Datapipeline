import requests
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='/opt/airflow/api-request/.env')
def fetch_data(api_url):
    """Fetches data from the Weather API URL."""
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        print("Data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise

def main_request():
    API_KEY = os.getenv("WEATHER_API_KEY")
    url = f"https://api.weatherstack.com/current?access_key={API_KEY}&query=Ha Noi"
    return fetch_data(url)

if __name__ == "__main__":
    main_request()