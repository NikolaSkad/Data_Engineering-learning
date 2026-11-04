import json
import os
import time

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from websocket import create_connection

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:9092")
TOPIC = os.environ.get("TOPIC", "subreddit-bluesky-en")
JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER.split(","),
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: v.encode("utf-8"),
        )
        print("Connected to Kafka!")
        break
    except KafkaTimeoutError as e:
        print(e)
        time.sleep(3)

ws = create_connection(JETSTREAM_URL)

try:
    while True:
        result = ws.recv()
        event = json.loads(result)

        if event["kind"] != "commit":
            continue
        if event["commit"]["operation"] != "create":
            continue

        record = event["commit"]["record"]
        if "en" not in record.get("langs", []):
            continue

        text = record.get("text", "")
        if not text:
            continue

        print("Received:", text)
        producer.send(topic=TOPIC, key=event["did"], value=text)
except KeyboardInterrupt:
    print("Stopped")
finally:
    producer.flush()
    producer.close()
    ws.close()
