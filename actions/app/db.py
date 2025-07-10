from clickhouse_driver import Client

from .models import TrackEvent

TRACK_COLUMNS = ["action_id", "user_id", "action_time", "action_type"]


class ClickHouseDB:
    def __init__(self, host: str) -> None:
        self.client = Client(host=host)

    def insert_track(self, data: TrackEvent):
        table_name = "actions.tracks"
        columns = ", ".join(data.model_dump().keys())
        insert_data = [tuple(data.model_dump().values())]
        output = self.client.execute(
            f"INSERT INTO {table_name} ({columns}) VALUES",
            insert_data
        )
        print(output)

    def start(self):
        print(self.client.execute("SHOW DATABASES"))

    def create_db(self):
        print(
            self.client.execute(
                "CREATE DATABASE IF NOT EXISTS actions ON CLUSTER action_cluster"
            )
        )

    def create_table(self):
        output = self.client.execute("""
            CREATE TABLE IF NOT EXISTS actions.tracks ON CLUSTER action_cluster
            (
                action_id UUID,
                action_time DateTime,
                user_id UUID,
                action_type Enum8(
                    'play' = 1,
                    'pause' = 2,
                    'skip' = 3,
                    'like' = 4,
                    'dislike' = 5,
                    'add_to_playlist' = 6,
                    'remove_from_playlist' = 7
                ),
                track_id UUID,
                recommended Nullable(Bool),
                playlist_id Nullable(UUID),
                duration Nullable(UInt32)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(action_time)
            ORDER BY (user_id, action_time, action_type);
            """)
        print(output)

    def select_all(self):
        print(self.client.execute("SELECT * FROM actions.tracks"))

if __name__ == "__main__":
    test_message = {
        "action_id": "a3a0f932-a655-4f7a-b50e-49247a44618d",
        "user_id": "8b935490-bd3c-4ed5-b47b-f59a5ad458cb",
        "action_time": "2025-07-10 09:23:30",
        "duration": 28,
        "track_id": "b79c306b-965e-4061-a318-d9505902a1df",
        "action_type": "dislike",
    }
    track_event = TrackEvent(**test_message)

    try:
        db = ClickHouseDB(host="localhost")
        db.start()
        # db.create_db()
        # db.create_table()
        # db.insert_track(track_event)
        db.select_all()
    except Exception as err:
        print(err)