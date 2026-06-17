# IoT Data Pipeline (MQTT → Kafka → PostgreSQL)

## Overview

A real-time telemetry pipeline for a mushroom fruiting chamber, built to learn
production-style data engineering and system design patterns end to end.

Real ESP32 devices publish sensor readings over MQTT. A Python service normalizes
those readings into a canonical event schema and produces them to Kafka. A second
service consumes from Kafka and writes time-series rows into PostgreSQL, which
Grafana queries for visualization.

```
ESP32 (ESPHome) → Mosquitto (MQTT) → mqtt_sub.py  → Kafka topic "sensor-events"
                                                   → kafka_consumer.py → PostgreSQL → Grafana
```

The Kafka stage decouples ingestion from storage, so the producer never blocks on
the database and a future stream-processing / analytics layer can read the same
topic without touching the ingest path.

---

## Goals

- Learn end-to-end IoT data ingestion on real hardware
- Design a clean canonical event schema for streaming systems
- Practice MQTT + Kafka + Python stream processing
- Store structured time-series data in PostgreSQL
- Keep the architecture open to a stream-processing / analytics layer

---

## Repository layout

```
esphome/                     ESPHome config for the ESP32-C6 sensor node
  esp32_c6.yaml
  secrets.yaml               (gitignored — wifi credentials)
firmware/
  esp32_cam_gc2145/          PlatformIO firmware for the GC2145/RHYX M21-45 camera
pipeline/
  mqtt_sub.py                MQTT subscriber → normalize → Kafka producer
  kafka_consumer.py          Kafka consumer → PostgreSQL sink
grafana/
  provisioning/datasources/  Auto-provisioned PostgreSQL data source for Grafana
docker-compose.yml           Single-node Kafka (KRaft mode) + Grafana
.env                         (gitignored) broker + Postgres credentials
```

---

## Devices

| Device           | Role                  | Notes                                              |
|------------------|-----------------------|----------------------------------------------------|
| ESP32-C6         | Sensor node (ESPHome) | Soil moisture, soil/air temp (Dallas), AHT20/DHT20 temp+humidity, BMP280 temp+pressure, BH1750 lux, uptime |
| ESP32-CAM (GC2145) | Camera              | Custom firmware; GC2145 can't do hardware JPEG, so frames are RGB565 → JPEG in software (`firmware/esp32_cam_gc2145`) |

The ESP32-C6 publishes to MQTT with `topic_prefix: iot/prod/esp32_c6` via the
ESPHome MQTT integration.

---

## MQTT topic convention

The pipeline consumes the topic shapes ESPHome publishes (see
`should_process()` in `pipeline/mqtt_sub.py`):

```
iot/prod/{device_id}/sensor/{slug}/state    # a sensor reading
iot/prod/{device_id}/status                 # device online / offline
```

### Examples

```
iot/prod/esp32_c6/sensor/dht20_temperature/state
iot/prod/esp32_c6/sensor/soil_moisture/state
iot/prod/esp32_c6/sensor/bh1750_illuminance/state
iot/prod/esp32_c6/status
```

The `{slug}` is derived from the ESPHome sensor name (e.g. `DHT20_Temperature`
→ `dht20_temperature`). Topics that don't match these shapes are ignored.

---

## Event schema (canonical format)

`mqtt_sub.py` normalizes every accepted message into this schema before producing
it to Kafka:

```json
{
  "ts": "2026-06-01T12:00:00Z",
  "device_id": "esp32_c6",
  "sensor": "dht20",
  "metric": "temperature",
  "value": 24.3,
  "unit": null,
  "topic": "iot/prod/esp32_c6/sensor/dht20_temperature/state",
  "event_type": "telemetry",
  "source": "mqtt"
}
```

Normalization rules:

- `sensor` / `metric` are split from `{slug}` on the first underscore
  (`soil_moisture` → sensor `soil`, metric `moisture`). A slug with no underscore
  becomes `sensor=slug`, `metric=null`.
- `status` topics map to `sensor="system"`, `metric="status"`.
- `value` is the payload parsed as `float`; non-numeric payloads become `null`.
- `unit` is currently always `null` (enrichment is a TODO).
- The raw `topic` is kept for traceability.

---

## PostgreSQL schema

`kafka_consumer.py` inserts into this table:

```sql
CREATE TABLE sensor_events (
    id         BIGSERIAL PRIMARY KEY,
    ts         TIMESTAMPTZ NOT NULL,
    device_id  TEXT,
    sensor     TEXT,
    metric     TEXT,            -- nullable: a slug with no underscore yields metric = NULL
    value      DOUBLE PRECISION,
    unit       TEXT,
    topic      TEXT,
    event_type TEXT,
    source     TEXT
);

-- Recommended once querying by device/time (not yet created):
-- CREATE INDEX idx_device_time ON sensor_events (device_id, ts DESC);
-- CREATE INDEX idx_metric_time ON sensor_events (metric, ts DESC);
```

---

## Running the pipeline

1. **Configure environment.** Create a `.env` (gitignored) with:

   ```
   MQTT_BROKER=192.168.10.13
   PG_HOST=...
   PG_DB=...
   PG_USER=...
   PG_PASSWORD=...
   ```

2. **Start Kafka and Grafana** (single-node KRaft + Grafana):

   ```bash
   docker compose up -d
   ```

3. **Start the Kafka → PostgreSQL sink:**

   ```bash
   python pipeline/kafka_consumer.py
   ```

4. **Start the MQTT → Kafka producer:**

   ```bash
   python pipeline/mqtt_sub.py
   ```

With both running, sensor readings flow from the ESP32 through MQTT and Kafka into
PostgreSQL.

5. **Visualize in Grafana.** Grafana comes up with `docker compose up` at
   `http://localhost:3000` (default login `admin` / `admin`). The PostgreSQL data
   source is auto-provisioned from `grafana/provisioning/` using the `.env`
   credentials, so you can build dashboards on the `sensor_events` table right away.

   The data source connects to `host.docker.internal:5432` to reach PostgreSQL
   running on the host. On Linux this resolves via the `host-gateway` mapping in
   `docker-compose.yml`.

### Testing without hardware

Publish a message in the shape `should_process()` accepts:

```bash
mosquitto_pub -h 192.168.10.13 \
  -t iot/prod/esp32_c6/sensor/dht20_temperature/state \
  -m 24.5
```

---

## Dependencies

**Python:** `paho-mqtt`, `kafka-python`, `psycopg2`, `python-dotenv`

**Infrastructure:** Mosquitto (MQTT broker), PostgreSQL 14+, and — via
`docker-compose.yml` — Apache Kafka and Grafana (reads from PostgreSQL)

---

## Current status

- [x] MQTT ingestion working
- [x] Python normalization layer
- [x] Kafka producer/consumer
- [x] PostgreSQL storage
- [x] Grafana dashboards (PostgreSQL data source)
- [ ] Stream processing layer (aggregations / anomaly detection)
- [ ] Unit enrichment in the canonical schema

---

## Design principles

- Keep the raw topic for traceability
- Normalize everything into one canonical schema
- Decouple devices from storage via MQTT and Kafka
- Add complexity only when needed

---

## Future upgrades

- Stream processing (Kafka Streams / ksqlDB) for real-time aggregations
- Storage evolution: TimescaleDB for time-series, ClickHouse for analytics scale
- Anomaly detection and alerting on the Grafana dashboards

---

## Notes

This project intentionally starts simple and evolves into a distributed data
system. The goal is learning the real-time data architecture patterns used in
production, with the mushroom chamber as a concrete, sensor-rich workload.
</content>
</invoke>
