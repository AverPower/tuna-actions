import logging
import os
from pathlib import Path
from threading import Thread
from uuid import UUID

import uvicorn
import yaml
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Query, HTTPException

from kafka import KafkaConsumer

from .models import PopularTrack, TrackStat
from .processor import ActionProcessor, AdHandler, TrackHandler
from .storage import Storage, get_db_client
from .topic_manage import add_topics

BASE_DIR = Path(__name__).parent
ENV_DIR = BASE_DIR / ".env"
LOG_CONFIG_DIR = BASE_DIR / "logging_config.yml"

load_dotenv(ENV_DIR)


with LOG_CONFIG_DIR.open("r") as log_fin:
    config = yaml.safe_load(log_fin)
logging.config.dictConfig(config)
logger = logging.getLogger(__name__)


app = FastAPI(
    title="Action Service for Tuna Music",
    description="API для выполнения аналитических запросов к ClickHouse",
)


@app.get(
    "/tracks/popular",
    summary="Популярные треки",
    tags=["Tracks"],
)
def get_popular_tracks_last_days(
    days: int = Query(7, description="Период в днях"),
    db: Storage = Depends(get_db_client),
) -> list[PopularTrack]:
    rows = db.get_poular_tracks(days)
    if rows is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return [PopularTrack(track_id=row[0], play_count=row[1]) for row in rows]


@app.get("/tracks/{track_id}/stats", summary="Статистика по треку", tags=["Tracks"])
def get_track_stats(track_id: UUID, db: Storage = Depends(get_db_client)) -> TrackStat:
    row = db.get_track_stats(track_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Item not found")
    row = row[0]
    return TrackStat(
        track_id=track_id, total_plays=row[0], avg_duration=row[1], unique_users=row[2]
    )


@app.get(
    "/users/{user_id}/top-tracks",
    summary="Самые прослушиваемые треки пользователя",
    tags=["Tracks"]
)
def get_user_top_tracks(
    user_id: int,
    limit: int = Query(5, le=50),
    db: Storage = Depends(get_db_client)
) -> list[PopularTrack]:
    rows = db.get_user_top_tracks(user_id, limit)
    if rows is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return [PopularTrack(track_id=row[0], play_count=row[1]) for row in rows]


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
    clickhouse_storage = get_db_client()
    clickhouse_storage.create_tables()
    action_processor = ActionProcessor(consumer=consumer, storage=clickhouse_storage)
    for handler in handlers:
        action_processor.register_handler(handler)
    process_thread = Thread(target=action_processor.run, daemon=True)
    process_thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
