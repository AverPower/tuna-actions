import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

import aiochclient
from aiohttp import ClientSession

DEFAULT_CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
DEFAULT_CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
DEFAULT_CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")

logger = logging.getLogger(__name__)

class Storage(ABC):

    @abstractmethod
    def __init__(self, host: str, db_name: str, port: str) -> None:
        ...

    @abstractmethod
    async def insert_row(self, data: dict, table_name: str)-> None:
        ...

    @abstractmethod
    async def __aenter__(self):
        ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        ...


class ClickHouseStorage(Storage):

    _db_created = False

    def __init__(self, host: str, db_name: str, port: str) -> None:
        self.host = host
        self.port = port
        self.url = f'http://{self.host}:{self.port}'
        self.db_name = db_name
        self._session = None
        self._client = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self) -> None:

        if not ClickHouseStorage._db_created:
            await self._create_db()
            ClickHouseStorage._db_created = True

        self._session = ClientSession()
        self._client = aiochclient.ChClient(
            self._session,
            url=self.url,
            database=self.db_name
        )

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
            self._client = None

    async def insert_row(self, data: dict, table_name: str) -> None:

        columns = ", ".join(data.keys())
        values = tuple(data.values())
        query = f"INSERT INTO {table_name} ({columns}) VALUES {values}"
        try:
            await self._client.execute(query)
            _msg = f"Inserted row into {table_name}"
            logger.debug(_msg)
        except Exception as e:
            _msg = f"Error inserting row: {e}"
            logger.error(_msg, exc_info=True)
            raise


    async def _create_db(self) -> None:
        try:
            temp_session = ClientSession()
            temp_client = aiochclient.ChClient(temp_session, url=self.url)
            query = "CREATE DATABASE IF NOT EXISTS actions ON CLUSTER action_cluster"
            await temp_client.execute(query)
            await temp_session.close()
            logger.debug("Created DB")
        except Exception as e:
            _msg = f"Error creating db: {e}"
            logger.error(_msg, exc_info=True)
            raise

    async def create_tables(self):
        try:
            query_ads = """
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
            """
            await self._client.execute(query_ads)
            query_tracks = """
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
            """
            await self._client.execute(query_tracks)
            logger.debug("Created tables")
        except Exception as e:
            _msg = f"Error creating tables: {e}"
            logger.error(_msg, exc_info=True)
            raise


    async def get_poular_tracks(self, days: int):
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
            rows = await self._client.execute(
                query,
                {"days": days}
            )
        except Exception as err:
            _msg = f"Get poular tracks error {err}"
            logger.error(_msg)
            rows = None
        finally:
            return rows


    async def get_track_stats(self, track_id: str):
        query = """
        SELECT
            count() as total_plays,
            toInt32(avg(duration)) as avg_duration,
            uniq(user_id) as unique_users
        FROM tracks
        WHERE track_id = %(track_id)s
            AND action_type = 'play'
        """
        try:
            rows = await self._client.execute(
                query,
                {"track_id": track_id}
            )
        except Exception as err:
            _msg = f"Get track stats error {err}"
            logger.error(_msg)
            rows = None
        finally:
            return rows

    async def get_user_top_tracks(self, user_id: str, limit: int):
        query = """
        SELECT
            track_id,
            count() as plays
        FROM tracks
        WHERE user_id = %(user_id)s
        GROUP BY track_id
        ORDER BY plays DESC
        LIMIT %(limit)s
        """

        try:
            rows = await self._client.execute(
                query,
                {"user_id": user_id, "limit": limit}
            )
        except Exception as err:
            _msg = f"Get user top tracks error {err}"
            logger.error(_msg)
            rows = None
        finally:
            return rows


    async def select_test(self):
        query_test_1 = "SELECT uniq(user_id) FROM tracks"
        query_test_2 = "SELECT COUNT(*) FROM ads"
        try:
            await self._client.execute(query_test_1)
            await self._client.execute(query_test_2)
        except Exception as err:
            _msg = f"Error testing: {err}"
            logger.error(_msg, exc_info=True)



async def get_db_client() -> Optional[ClickHouseStorage]:
    return ClickHouseStorage(
        host=DEFAULT_CLICKHOUSE_HOST,
        port=DEFAULT_CLICKHOUSE_PORT,
        db_name=DEFAULT_CLICKHOUSE_DB
    )
