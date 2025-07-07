from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import UUID4, BaseModel, field_serializer


class BaseAction(BaseModel):
    action_id: UUID4
    user_id: UUID4
    timestamp: datetime
    context: dict = None

    @field_serializer("timestamp")
    def serialize_timestamp(self, ts: datetime) -> str:
        return ts.strftime("%Y-%m-%d %H:%M:%S")


class TrackEventType(str, Enum):
    play = "play"
    pause = "pause"
    skip = "skip"
    like = "like"
    dislike = "dislike"
    add_to_playlist = "add_to_playlist"
    remove_from_playlist = "remove_from_playlist"


class TrackEvent(BaseAction):
    action_type: TrackEventType
    track_id: UUID4
    recommended: bool = False
    playlist_id: Optional[UUID4] = None
    duration: Optional[int] = None


class AdEventType(str, Enum):
    play = "play"
    pause = "pause"


class AdEvent(BaseAction):
    action_type: AdEventType
    duration: Optional[int] = None
    # clicked: Optional[bool] = None
