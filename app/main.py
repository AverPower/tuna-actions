import logging
import os
from pathlib import Path
from uuid import UUID

import sentry_sdk
import uvicorn
import yaml
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query
from models import PopularTrack, TrackEvent, TrackStat
from producer import AIOKafkaProducer, get_producer
from storage import Storage, get_db_client

load_dotenv(".env")
SENTRY_DSN = os.getenv("SENTRY_DSN")

BASE_DIR = Path(__name__).parent
LOG_CONFIG_DIR = BASE_DIR / "logging_config.yml"


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))


with LOG_CONFIG_DIR.open("r") as log_fin:
    config = yaml.safe_load(log_fin)
logging.config.dictConfig(config)
logger = logging.getLogger(__name__)


sentry_sdk.init(
    dsn=SENTRY_DSN,
    # Add data like request headers and IP for users,
    # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
    send_default_pii=True,
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for tracing.
    traces_sample_rate=1.0,
    # Set profile_session_sample_rate to 1.0 to profile 100%
    # of profile sessions.
    profile_session_sample_rate=1.0,
    # Set profile_lifecycle to "trace" to automatically
    # run the profiler on when there is an active transaction
    profile_lifecycle="trace",
)


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
    tags=["Tracks"],
)
def get_user_top_tracks(
    user_id: UUID, limit: int = Query(5, le=50), db: Storage = Depends(get_db_client)
) -> list[PopularTrack]:
    rows = db.get_user_top_tracks(user_id, limit)
    if rows is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return [PopularTrack(track_id=row[0], play_count=row[1]) for row in rows]


@app.post("/tracks", summary="Создать событие типа _Трек_", tags=["Tracks"])
async def create_track_event(
    track_event: TrackEvent, producer: AIOKafkaProducer = Depends(get_producer)
) -> dict:
    track_data = track_event.model_dump_json()
    try:
        producer.send_and_wait(
            topic="track", value=track_data, key=str(track_event.user_id)
        )
        _msg = f"Message sent to track topic : {track_data}"
        logger.info(_msg)
        return {"status": "success", "message": "Data sent to Kafka"}
    except Exception as e:
        _msg = f"Error while sending to Kafka: {str(e)}"
        logger.error(_msg)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    clickhouse_storage = get_db_client()
    clickhouse_storage.create_tables()
    uvicorn.run(app, host="0.0.0.0", port=8000)
