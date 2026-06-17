import json
import paho.mqtt.client as mqtt
from datetime import  datetime, timezone
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

BROKER = os.environ["MQTT_BROKER"]
TOPIC = "iot/prod/#"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

def on_connect(client, userdata, flags, reason_code, properties=None):
    print("connected:", reason_code)
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    if not should_process(topic):
        return
    try:
        event = normalize(topic, payload)
        print(json.dumps(event, default=str))
        producer.send('sensor-events', value=event)
    except Exception as e:
        print(f"failed to process message: {e}, topic={topic}, payload={payload}")

def should_process(topic: str) -> bool:
    parts = topic.split("/")
    # sensor reading: sensors/{device}/sensor/{slug}/state
    if len(parts) == 6 and parts[3] == "sensor" and parts[5] == "state":
        return True
    # device online/offline
    if len(parts) == 4 and parts[3] == "status":
        return True
    return False

def normalize(topic: str, payload: str):
    parts = topic.split("/")
    device_id = parts[2]


    if parts[3] == "status":  
        sensor, metric = "system", "status"
    else:
        slug = parts[4]
        idx = slug.find("_")
        if idx != -1:
            sensor = slug[:idx]
            metric = slug[idx + 1:]
        else:
            sensor, metric = slug, None

    value = payload
    try:
        value = float(payload)
    except (ValueError, TypeError):
        pass

    return {
        "ts": datetime.now(timezone.utc),
        "device_id": device_id,
        "sensor": sensor,
        "metric": metric,
        "value": value if isinstance(value, float) else None,
        "unit": None,
        "topic": topic,
        "event_type": "telemetry",
        "source": "mqtt",
    }

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, 1883, 60)
client.loop_forever()
