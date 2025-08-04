import asyncio
from logging import config, getLogger
import os
from pathlib import Path

import yaml
from aiokafka import AIOKafkaConsumer
from processor import ActionProcessor, AdHandler, TrackHandler
from storage import get_db_client
from topic_manage import add_topics

BASE_DIR = Path(__name__).parent
LOG_CONFIG_DIR = BASE_DIR / "logging_config.yml"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(",")
TOPICS = os.getenv("DEFAULT_TOPICS").split(",")


with LOG_CONFIG_DIR.open("r") as log_fin:
    file_config = yaml.safe_load(log_fin)

config.dictConfig(file_config)
logger = getLogger(__name__)


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
    clickhouse_storage = await get_db_client()

    action_processor = ActionProcessor(consumer=consumer, storage=clickhouse_storage)
    for handler in handlers:
        await action_processor.register_handler(handler)
    try:
        await action_processor.run()
    except KeyboardInterrupt:
        await action_processor.stop()


if __name__ == "__main__":
    asyncio.run(main())
