import json
import os
import time

import psycopg2
from kafka import KafkaConsumer


KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
TOPIC = os.getenv("TOPIC", "weather.raw")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_pass")


def get_alert(weather):
    alerts = []

    temperature = weather.get("temperature")
    wind_speed = weather.get("wind_speed")
    rain = weather.get("rain")

    if temperature is not None and temperature >= 30:
        alerts.append("HOT_WEATHER")

    if temperature is not None and temperature <= 10:
        alerts.append("COLD_WEATHER")

    if wind_speed is not None and wind_speed >= 30:
        alerts.append("STRONG_WIND")

    if rain is not None and rain > 0:
        alerts.append("RAIN")

    if alerts:
        return "ALERT", ", ".join(alerts)

    return "NORMAL", None


def connect_to_postgres():
    while True:
        try:
            connection = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            print("Connected to PostgreSQL")
            return connection

        except Exception as error:
            print("PostgreSQL is not ready yet:", error)
            print("Retrying in 5 seconds...")
            time.sleep(5)


def save_weather(connection, weather):
    status, alert = get_alert(weather)

    sql = """
        INSERT INTO weather_readings (
            city,
            country,
            weather_time,
            temperature,
            humidity,
            wind_speed,
            rain,
            status,
            alert,
            produced_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    values = (
        weather.get("city"),
        weather.get("country"),
        weather.get("time"),
        weather.get("temperature"),
        weather.get("humidity"),
        weather.get("wind_speed"),
        weather.get("rain"),
        status,
        alert,
        weather.get("produced_at"),
    )

    with connection.cursor() as cursor:
        cursor.execute(sql, values)

    connection.commit()

    print(
        f"Saved: {weather.get('city')} | "
        f"temp={weather.get('temperature')} | "
        f"wind={weather.get('wind_speed')} | "
        f"rain={weather.get('rain')} | "
        f"status={status} | "
        f"alert={alert}"
    )


print(f"Consumer started. Reading from Kafka: {KAFKA_SERVER}, topic: {TOPIC}")

db_connection = connect_to_postgres()

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="weather-DB-consumers",
)

for message in consumer:
    try:
        weather = json.loads(message.value.decode("utf-8"))
        save_weather(db_connection, weather)

    except json.JSONDecodeError:
        print("Invalid JSON message:", message.value)

    except Exception as error:
        print("Failed to process message:", error)
        db_connection.rollback()
