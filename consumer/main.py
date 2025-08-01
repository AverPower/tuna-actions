import asyncio
import os

from aiokafka import AIOKafkaConsumer
from processor import ActionProcessor, AdHandler, TrackHandler
from storage import get_db_client
from topic_manage import add_topics

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(",")
TOPICS = os.getenv("DEFAULT_TOPICS").split(",")


async def main() -> None:

    add_topics(KAFKA_BOOTSTRAP_SERVERS, TOPICS)

    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
    )

    handlers = [
        TrackHandler(topic_name="track", table_name="tracks"),
        AdHandler(topic_name="ad", table_name="ads"),
    ]
    clickhouse_storage = get_db_client()
    action_processor = ActionProcessor(consumer=consumer, storage=clickhouse_storage)
    for handler in handlers:
        await action_processor.register_handler(handler)
    try:
        await action_processor.run()
    except KeyboardInterrupt:
        await action_processor.stop()


if __name__ == "__main__":
    asyncio.run(main())
