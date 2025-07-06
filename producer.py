from time import sleep
from uuid import uuid4

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=["localhost:9094"])

for _ in range(10):
    producer.send(
        topic="actions",
        value=f"skip track {uuid4()}".encode("utf-8)"),
        key=b"user_id",
    )
    sleep(1)
