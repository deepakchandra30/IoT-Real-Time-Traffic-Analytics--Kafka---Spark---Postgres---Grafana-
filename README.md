# IoT Real-Time Traffic Analytics (Kafka + Spark + Postgres + Grafana)

## What this project does
Simulates IoT traffic sensors that emit events (vehicle count, avg speed, occupancy) to Kafka.
Spark Structured Streaming consumes the stream, cleans it, computes windowed aggregates,
detects anomalies, and writes results to Postgres. Grafana visualizes the results.

### Analytics included
- Filtering/cleaning: drops invalid values and out-of-range readings
- Window analytics: 1-minute and 5-minute tumbling windows
- Correlation-ready outputs: aggregates include speed vs count time series by sensor
- Anomaly detection: robust z-score on speed per sensor (approx rolling mean/std)

## Prerequisites
- Docker Desktop (or Docker Engine) with Docker Compose v2

## Quick start
1. Create a folder and add all files exactly as provided.
2. Start:
   ```bash
   docker compose up --build
   ```
3. Wait ~1–2 minutes for everything to initialize.

## Access
- Grafana: http://localhost:3000
  - user: `admin`
  - pass: `admin`
- Kafka is internal; Spark job and producer connect via Docker network.
- Postgres: localhost:5432
  - user: `postgres`
  - pass: `postgres`
  - db: `traffic`

## Grafana dashboard
A sample dashboard JSON is included in `grafana/dashboards/traffic-dashboard.json`.
It is automatically provisioned (no manual import needed).

## Tables created in Postgres
- `traffic_raw` (optional debug sink — not used by default)
- `traffic_agg_1m`
- `traffic_agg_5m`
- `traffic_anomalies`

## How to demonstrate evaluation
- Change producer rate via env `EVENTS_PER_SEC`
- Inject incidents with `INCIDENT_PROBABILITY` to simulate anomalies
- Measure throughput by counting rows per minute in `traffic_agg_1m`
- Measure latency by comparing Kafka event_time vs Spark processing_time (included in anomaly table)

## Useful commands
Rebuild only Spark:
```bash
docker compose build spark
docker compose up spark
```

Stop + remove volumes:
```bash
docker compose down -v
```

## Notes for your report
- Include the architecture diagram: Producer → Kafka → Spark → Postgres → Grafana
- Explain event-time windows and watermarking
- Evaluate: throughput, latency, anomaly rate, resource usage (docker stats)