import logging
import os
from pathlib import Path
from threading import Thread

import uvicorn
import yaml
from dotenv import load_dotenv
from fastapi import FastAPI

from .consumer import ActionKafkaConsumer, AdHandler, TrackHandler
from .topic_manage import add_topics

BASE_DIR = Path(__name__).parent
ENV_DIR = BASE_DIR / ".env"
LOG_CONFIG_DIR = BASE_DIR / "logging_config.yml"

load_dotenv(ENV_DIR)
topics = os.getenv("DEFAULT_TOPICS").split(",")


with LOG_CONFIG_DIR.open("r") as log_fin:
    config = yaml.safe_load(log_fin)
logging.config.dictConfig(config)
logger = logging.getLogger(__name__)


app = FastAPI()


if __name__ == "__main__":
    add_topics(topics)
    consumer = ActionKafkaConsumer(topics)
    handlers = [TrackHandler("track"), AdHandler("ad")]
    for handler in handlers:
        consumer.register_handler(handler)
    consumer_thread = Thread(target=consumer.run, daemon=True)
    consumer_thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
