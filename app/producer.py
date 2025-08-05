import os

from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(",")


async def get_kafka_producer() -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
