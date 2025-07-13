import json
from datetime import datetime
from random import choice, choices
from time import sleep
from uuid import uuid4

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=["localhost:9094"])

topics = ["track", "ad"]
track_action_types = [
    "play",
    "pause",
    "skip",
    "like",
    "dislike",
    "add_to_playlist",
    "remove_from_playlist",
]
ad_action_types = ["play", "pause"]
users = [uuid4() for _ in range(100)]
tracks = [uuid4() for _ in range(1000)]
playlists = [uuid4() for _ in range(100)]
ads = [uuid4() for _ in range(100)]

while True:
    topic = str(choices(topics, weights=(70, 30), k=1)[0])
    action_id = str(uuid4())
    track = str(choice(tracks))
    ad = str(choice(ads))
    user = str(choice(users))
    playlist = str(choice(playlists))
    duration = choice(range(1, 60))
    message = {
        "action_id": action_id,
        "user_id": user,
        "action_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "duration": duration,
    }
    match topic:
        case "track":
            message["track_id"] = track
            action_type = choice(track_action_types)
            message["action_type"] = action_type
            recommended = choice([True, False])
            if recommended:
                message["recommended"] = True
            if "playlist" in action_type:
                message["playlist_id"] = playlist
        case "ad":
            message["ad_id"] = ad
            action_type = choice(ad_action_types)
            message["action_type"] = action_type
        case _:
            continue

    producer.send(topic=topic, value=json.dumps(message).encode("utf-8"), key=user.encode("utf-8"))
    print(f"Sent {action_id} message for topic {topic}")
    sleep(1)
