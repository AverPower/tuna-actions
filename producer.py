import os
import json
from time import sleep
from uuid import uuid4
from random import choice
from datetime import datetime

from kafka import KafkaProducer
from dotenv import load_dotenv


load_dotenv("actions/app/.env")


producer = KafkaProducer(bootstrap_servers=["localhost:9094"])

topics = os.getenv("DEFAULT_TOPICS").split(",")
users = [uuid4() for _ in range(100)]
tracks = [uuid4() for _ in range(1000)]
playlists = [uuid4() for _ in range(100)]
ads = [uuid4() for _ in range(100)]

while True:
    topic = str(choice(topics))
    track = str(choice(tracks))
    ad = str(choice(ads))
    user = str(choice(users))
    message = {"user_id": user, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    match topic:
        case "track_like":
            message["track_id"] = track
            recommended = choice([True, False])
            if recommended:
                message["recommended"] = True
        case "track_skip":
            message["track_id"] = track
            recommended = choice([True, False])
            if recommended:
                message["recommended"] = True
        case "ad":
            message["ad_id"] = ad
        case _:
            continue

    producer.send(
        topic=topic,
        value=json.dumps(message),
        key=user
    )
    sleep(1)
