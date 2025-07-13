import logging
from abc import ABC, abstractmethod

from clickhouse_driver import Client

logger = logging.getLogger(__name__)


class Storage(ABC):

    @abstractmethod
    def __init__(self, host: str, db_name: str) -> None:
        ...

    @abstractmethod
    def insert_row(self, data: dict, table_name: str)-> None:
        ...


class ClickHouseStorage(Storage):
    def __init__(self, host: str, db_name: str) -> None:
        self.client = Client(host=host, database=db_name)

    def insert_row(self, data: dict, table_name: str) -> None:
        columns = ", ".join(data.keys())
        insert_data = [tuple(data.values())]
        output = self.client.execute(
            f"INSERT INTO {table_name} ({columns}) VALUES",
            insert_data
        )
        logger.debug(output)


    def create_db(self) -> None:
        print(
            self.client.execute(
                "CREATE DATABASE IF NOT EXISTS actions ON CLUSTER action_cluster"
            )
        )

    def create_tables(self):
        result = self.client.execute("""
            CREATE TABLE IF NOT EXISTS actions.ads ON CLUSTER action_cluster
            (
                action_id UUID,
                action_time DateTime,
                user_id UUID,
                action_type Enum8(
                    'play' = 1,
                    'pause' = 2
                ),
                ad_id UUID,
                duration Nullable(UInt32)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(action_time)
            ORDER BY (user_id, action_time, action_type);
            """)
        _msg = f"TABLE CREATION {result}"
        logger.debug(_msg)
        result = self.client.execute("""
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
        _msg = f"TABLE CREATION {result}"
        logger.debug(_msg)


    def select_all(self):
        print(self.client.execute("SELECT COUNT(*) FROM tracks"))
        print(self.client.execute("SELECT COUNT(*) FROM ads"))

if __name__ == "__main__":
    try:
        db = ClickHouseStorage(host="localhost", db_name="actions")
        db.select_all()
    except Exception as err:
        print(err)