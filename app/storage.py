import logging
import os
from abc import ABC, abstractmethod

from clickhouse_driver import Client

DEFAULT_CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
DEFAULT_CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
DEFAULT_CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")

logger = logging.getLogger(__name__)


class Storage(ABC):

    @abstractmethod
    def __init__(self, host: str, db_name: str, port: str) -> None:
        ...

    @abstractmethod
    def insert_row(self, data: dict, table_name: str)-> None:
        ...


class ClickHouseStorage(Storage):
    def __init__(self, host: str, db_name: str, port: str) -> None:
        self.client = Client(host=host, port=port)
        self.create_db()
        self.client = Client(host=host, database=db_name, port=port)

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

    def get_poular_tracks(self, days: int):
        query = """
        SELECT
            track_id,
            count() as play_count
        FROM tracks
        WHERE action_type = 'play'
        AND action_time >= now() - interval %(days)s day
        GROUP BY track_id
        ORDER BY play_count DESC
        LIMIT 10;
        """
        try:
            rows = self.client.execute(
                query,
                {"days": days}
            )
        except Exception as err:
            _msg = f"Get poular tracks error {err}"
            logger.error(_msg)
            rows = None
        finally:
            return rows

    def get_track_stats(self, track_id: str):
        query = """
        SELECT
            count() as total_plays,
            avg(duration) as avg_duration,
            uniq(user_id) as unique_users
        FROM tracks
        WHERE track_id = %(track_id)s
        """
        try:
            rows = self.client.execute(
                query,
                {"track_id": track_id}
            )
        except Exception as err:
            _msg = f"Get track stats error {err}"
            logger.error(_msg)
            rows = None
        finally:
            return rows

    def get_user_top_tracks(self, user_id: str, limit: int):
        query = """
        SELECT 
            ptrack_id,
            count() as plays
        FROM tracks
        WHERE user_id = %(user_id)s
        GROUP BY track_id
        ORDER BY plays DESC
        LIMIT %(limit)s
        """

        try:
            rows = self.client.execute(
                query,
                {"user_id": user_id, "limit": limit}
            )
        except Exception as err:
            _msg = f"Get user top tracks error {err}"
            logger.error(_msg)
            rows = None
        finally:
            return rows

    def select_all(self):
        print(self.client.execute("SELECT uniq(user_id) FROM tracks"))
        print(self.client.execute("SELECT COUNT(*) FROM ads"))



def get_db_client() -> ClickHouseStorage:
    return ClickHouseStorage(
        host=DEFAULT_CLICKHOUSE_HOST,
        port=DEFAULT_CLICKHOUSE_PORT,
        db_name=DEFAULT_CLICKHOUSE_DB
        )


if __name__ == "__main__":
    try:
        db = get_db_client()
        db.select_all()
    except Exception as err:
        print(err)