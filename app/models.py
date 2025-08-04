from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import UUID4, BaseModel, field_serializer


class BaseAction(BaseModel):
    action_id: UUID4
    action_time: datetime
    user_id: UUID4

    @field_serializer('action_time', when_used='json')
    def serialize_dt(self, dt) -> str:
        return dt.strftime('%Y-%m-%d %H:%M:%S')


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
    ad_id: UUID4
    duration: Optional[int] = None


class PopularTrack(BaseModel):
    track_id: UUID4
    play_count: int

class TrackStat(BaseModel):
    track_id: UUID4
    total_plays : int
    avg_duration: int
    unique_users: int

