# IoT Real-Time Traffic Analytics Pipeline

## Overview
This project is an end-to-end IoT data analytics system designed to ingest, process, and visualise streaming traffic data in real time. It simulates a network of IoT traffic sensors, streams the events through a distributed message broker, processes them using a micro-batch streaming framework, and visualises the metrics on an interactive dashboard.

## Dataset Description
Because high-throughput municipal traffic data is restricted, this project uses a custom Python simulator (`producer/app.py`) as the IoT data source. It generates continuous telemetry (vehicle counts, average speeds, lane occupancy) based on Gaussian distributions and pushes it to Kafka at a configurable rate (~20 events/sec). The simulator also supports injecting artificial anomalies (traffic jams) to test the pipeline's statistical reliability.

## Analytics Techniques Applied
The Spark Structured Streaming job (`spark/job.py`) performs the following real-time analytics:
* **Data Filtering:** Drops corrupted sensor readings (e.g., negative speeds or out-of-range occupancy percentages).
* **Window-Based Aggregations:** Computes vehicle counts and average speeds over 1-minute and 5-minute tumbling windows using strict event-time processing and watermarking.
* **Anomaly Detection:** Maintains a rolling 5-minute mean and standard deviation for each sensor. Incoming events are evaluated using a Z-score, immediately flagging `SPEED_DROP` or `SPEED_SPIKE` incidents if the Z-score exceeds an absolute value of 3.0.

## Technical Infrastructure
The system uses a microservices architecture orchestrated with **Docker Compose** to ensure full reproducibility and strict dependency isolation.
* **Ingestion Layer:** Apache Kafka (with Zookeeper)
* **Stream Processing:** Apache Spark Structured Streaming (PySpark)
* **Storage Sink:** PostgreSQL (Relational tables with unique time-window primary keys)
* **Visualisation:** Grafana

## How to Run the Project (Reproducibility)

### Prerequisites
* Docker Engine and Docker Compose installed.
* Ports `9092`, `5432`, `3000`, and `4040` available on the host machine.

### 1. Start the Pipeline
Open a terminal in the root directory of the project and start all services in detached mode:

```bash
docker compose up -d --build
```

Note: Please wait 2–3 minutes for the Kafka broker to initialise, the Spark job to download its Ivy dependencies, and the Postgres tables to be created. Because the system uses an event-time watermark, data will appear on the dashboard after the first time window completes.

### 2. Access the System
* Grafana Dashboard: http://localhost:3000  
  Username: `admin`  
  Password: `admin`
* Spark Web UI (Job Metrics): http://localhost:4040
* PostgreSQL: `localhost:5432` (User: `postgres`, Pass: `postgres`, DB: `traffic`)

Note: If the dashboard does not provision automatically, click **+ Create -> Import**, and paste the raw JSON contents of `grafana/dashboards/traffic-dashboard.json`.

## Evaluating the System (Triggering Anomalies)
By default, the system runs with normal traffic patterns to establish a baseline. To demonstrate and evaluate the real-time anomaly detection:

1. Open the `.env` file in the project root.
2. Change the anomaly probability from `2%` to `80%`:

```env
INCIDENT_PROBABILITY=0.80
```

3. Save the file and recreate the producer container to apply the new configuration:

```bash
docker compose up -d producer
```

4. Observe the Grafana dashboard. The **Anomalies** table at the bottom will immediately start logging `SPEED_DROP` events. After the 2-minute Spark watermark passes, the 1-minute and 5-minute moving average line charts will dynamically update to reflect the massive drop in speed.

## Stopping and Cleaning the Environment
To safely stop all containers and wipe the database volumes for a fresh run:

```bash
docker compose down -v
```
