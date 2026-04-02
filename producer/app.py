import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "traffic_iot")

EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "20"))
SENSORS = int(os.getenv("SENSORS", "12"))
INCIDENT_PROB = float(os.getenv("INCIDENT_PROBABILITY", "0.02"))

# A few fake locations to make dashboards nicer
LOCATIONS = [
    "Dublin-City-Centre",
    "Dublin-M50-J7",
    "Dublin-Port-Tunnel",
    "Dublin-Blackrock",
    "Dublin-Blanchardstown",
]

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def make_event(sensor_idx: int):
    now = datetime.now(timezone.utc)

    sensor_id = f"S{sensor_idx:03d}"
    location = LOCATIONS[sensor_idx % len(LOCATIONS)]

    # baseline traffic patterns
    base_speed = random.gauss(55, 8)          # km/h
    base_vehicles = int(clamp(random.gauss(12, 5), 0, 60))
    base_occ = clamp(random.gauss(35, 15), 0, 100)

    incident = random.random() < INCIDENT_PROB

    # Inject incident-like behavior: speed drops, occupancy rises, vehicles may spike
    if incident:
        base_speed = clamp(base_speed - random.uniform(25, 45), 0, 120)
        base_occ = clamp(base_occ + random.uniform(20, 55), 0, 100)
        base_vehicles = int(clamp(base_vehicles + random.uniform(10, 25), 0, 120))

    # occasional sensor noise / outliers (to demonstrate filtering)
    if random.random() < 0.003:
        base_speed = random.choice([-10, 250])  # invalid values

    payload = {
        "event_time": now.isoformat(),
        "sensor_id": sensor_id,
        "location": location,
        "vehicle_count": int(base_vehicles),
        "avg_speed_kmh": float(round(base_speed, 2)),
        "occupancy_pct": float(round(base_occ, 2)),
        "incident": bool(incident),
    }
    return payload

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        acks=1,
    )

    interval = 1.0 / max(EVENTS_PER_SEC, 0.1)
    print(f"Producing to {TOPIC} via {KAFKA_BOOTSTRAP} at ~{EVENTS_PER_SEC} events/sec...")

    i = 0
    while True:
        sensor_idx = i % SENSORS
        evt = make_event(sensor_idx)
        producer.send(TOPIC, evt)
        i += 1
        time.sleep(interval)

if __name__ == "__main__":
    main()