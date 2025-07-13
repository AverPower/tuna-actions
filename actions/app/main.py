import logging
import os
from pathlib import Path
from threading import Thread

import uvicorn
import yaml
from dotenv import load_dotenv
from fastapi import FastAPI

from kafka import KafkaConsumer

from .processor import ActionProcessor, AdHandler, TrackHandler
from .storage import ClickHouseStorage
from .topic_manage import add_topics

BASE_DIR = Path(__name__).parent
ENV_DIR = BASE_DIR / ".env"
LOG_CONFIG_DIR = BASE_DIR / "logging_config.yml"

load_dotenv(ENV_DIR)


with LOG_CONFIG_DIR.open("r") as log_fin:
    config = yaml.safe_load(log_fin)
logging.config.dictConfig(config)
logger = logging.getLogger(__name__)


app = FastAPI()


if __name__ == "__main__":
    topics = os.getenv("DEFAULT_TOPICS").split(",")
    add_topics(topics)
    consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=["localhost:9094"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
        )
    handlers = [
        TrackHandler(topic_name="track", table_name="tracks"),
        AdHandler(topic_name="ad", table_name="ads"),
    ]
    clickhouse_storage = ClickHouseStorage(host="localhost", db_name="actions")
    clickhouse_storage.create_tables()
    action_processor = ActionProcessor(consumer=consumer, storage=clickhouse_storage)
    for handler in handlers:
        action_processor.register_handler(handler)
    process_thread = Thread(target=action_processor.run, daemon=True)
    process_thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
