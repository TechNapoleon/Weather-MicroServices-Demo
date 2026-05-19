import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()


KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
TOPIC = os.getenv("TOPIC", "weather.raw")
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "30"))


CITIES = [
    {"name": "Haifa", "lat": 32.7940, "lon": 34.9896},
    {"name": "Tel Aviv", "lat": 32.0853, "lon": 34.7818},
    {"name": "Jerusalem", "lat": 31.7683, "lon": 35.2137},
    {"name": "Beer Sheva", "lat": 31.2529, "lon": 34.7915},
    {"name": "Eilat", "lat": 29.5577, "lon": 34.9519},
    {"name": "Tiberias", "lat": 32.7959, "lon": 35.5310},
    {"name": "Nazareth", "lat": 32.6996, "lon": 35.3035},
    {"name": "Ashdod", "lat": 31.8044, "lon": 34.6553},
]


def get_weather(city):
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,rain",
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    current = response.json()["current"]

    return {
        "city": city["name"],
        "country": "Israel",
        "time": current["time"],
        "temperature": current["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
        "rain": current["rain"],
        "produced_at": datetime.now(timezone.utc).isoformat(),
    }


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda data: json.dumps(data).encode("utf-8"),
)

print(f"Producer started. Sending messages to {KAFKA_SERVER}, topic: {TOPIC}")

while True:
    for city in CITIES:
        try:
            weather = get_weather(city)
            producer.send(TOPIC, weather)
            print("Sent:", weather)

        except requests.exceptions.RequestException as error:
            print(f"Failed to fetch weather for {city['name']}: {error}")

        except Exception as error:
            print(f"Unexpected error for {city['name']}: {error}")

    producer.flush()
    print(f"Sleeping for {SLEEP_SECONDS} seconds...\n")
    time.sleep(SLEEP_SECONDS)
