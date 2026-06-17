from uu import Error
from kafka import KafkaConsumer
import json
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    dbname=os.environ["PG_DB"],
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    host=os.environ["PG_HOST"],
    port=5432,
)
cur = conn.cursor()

def insert_db(event):
    cur.execute("""
        INSERT INTO sensor_events (ts, device_id, sensor, metric, value, unit, topic, event_type, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        event["ts"],
        event["device_id"],
        event["sensor"],
        event["metric"],
        event["value"],
        event["unit"],
        event["topic"],
        event["event_type"],
        event["source"],
    ))
    conn.commit()

consumer = KafkaConsumer(
    'sensor-events',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in consumer:
    event = message.value
    try:
        insert_db(event)
    except Exception as e:
        print(f"failed to insert event: {e}, event={event}")
