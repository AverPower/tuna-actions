import os
from threading import Thread

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

from .consumer import ActionKafkaConsumer
from .topic_manage import add_topics

load_dotenv(".env")
topics = os.getenv("DEFAULT_TOPICS")

app = FastAPI()


if __name__ == "__main__":
    add_topics(topics)
    consumer = ActionKafkaConsumer(topics)
    consumer_thread = Thread(target=consumer.run, daemon=True)
    consumer_thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
