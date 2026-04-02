-- Raw events table (optional; not used by default but useful for debugging)
CREATE TABLE IF NOT EXISTS traffic_raw (
  event_time TIMESTAMPTZ NOT NULL,
  sensor_id  TEXT NOT NULL,
  location   TEXT NOT NULL,
  vehicle_count INT NOT NULL,
  avg_speed_kmh  DOUBLE PRECISION NOT NULL,
  occupancy_pct  DOUBLE PRECISION NOT NULL,
  incident BOOLEAN NOT NULL,
  ingestion_time TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 1-minute aggregates
CREATE TABLE IF NOT EXISTS traffic_agg_1m (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  sensor_id    TEXT NOT NULL,
  location     TEXT NOT NULL,
  events       INT NOT NULL,
  vehicles_sum BIGINT NOT NULL,
  speed_avg    DOUBLE PRECISION NOT NULL,
  speed_min    DOUBLE PRECISION NOT NULL,
  speed_max    DOUBLE PRECISION NOT NULL,
  occupancy_avg DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (window_start, window_end, sensor_id)
);

-- 5-minute aggregates
CREATE TABLE IF NOT EXISTS traffic_agg_5m (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  sensor_id    TEXT NOT NULL,
  location     TEXT NOT NULL,
  events       INT NOT NULL,
  vehicles_sum BIGINT NOT NULL,
  speed_avg    DOUBLE PRECISION NOT NULL,
  speed_min    DOUBLE PRECISION NOT NULL,
  speed_max    DOUBLE PRECISION NOT NULL,
  occupancy_avg DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (window_start, window_end, sensor_id)
);

-- Anomalies table
CREATE TABLE IF NOT EXISTS traffic_anomalies (
  event_time TIMESTAMPTZ NOT NULL,
  processing_time TIMESTAMPTZ NOT NULL,
  sensor_id TEXT NOT NULL,
  location  TEXT NOT NULL,
  avg_speed_kmh DOUBLE PRECISION NOT NULL,
  vehicle_count INT NOT NULL,
  occupancy_pct DOUBLE PRECISION NOT NULL,
  speed_z DOUBLE PRECISION NOT NULL,
  reason TEXT NOT NULL
);