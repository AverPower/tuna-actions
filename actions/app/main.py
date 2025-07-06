from threading import Thread

import uvicorn
from fastapi import FastAPI

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "actions",
    bootstrap_servers=["localhost:9094"],
    auto_offset_reset="earliest",
#    group_id="group-id",  # TODO CHANGE GROUP ID
)


def consumer_run(consumer: KafkaConsumer):
    try:
        for message in consumer:
            print(message.value)
    except Exception as err:
        print(err)


app = FastAPI()


if __name__ == "__main__":
    consumer_thread = Thread(target=consumer_run, args=(consumer))
    consumer_thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
