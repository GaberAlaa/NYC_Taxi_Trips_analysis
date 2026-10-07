from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

while True:
    event = {
        "location": random.choice(["A", "B", "C"]),
        "vehicle_count": random.randint(1, 100),
        "timestamp": time.time(),
    }

    producer.send("traffic", event)
    producer.flush()
    time.sleep(1)
