CREATE TABLE IF NOT EXISTS weather_readings (
    id BIGSERIAL PRIMARY KEY,

    city TEXT NOT NULL,
    country TEXT NOT NULL,

    weather_time TIMESTAMP,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    rain DOUBLE PRECISION,

    status TEXT,
    alert TEXT,

    produced_at TIMESTAMPTZ,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weather_city_time
ON weather_readings (city, consumed_at DESC);
