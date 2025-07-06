from datetime import datetime

from pydantic import UUID4, BaseModel


class UserAction(BaseModel):
    user_id: UUID4
    timestamp: datetime
    context: dict = None

class TrackAction(UserAction):
    track_id: UUID4
    recommended: bool = False


class SkipTrackEvent(TrackAction):
    ...

class LikeTrackEvent(TrackAction):
    like: bool


class PlaylistAddTrackEvent(TrackAction):
    playlist_id: UUID4


class SearchEvent(UserAction):
    search_line: dict

class SearchClickEvent(TrackAction):
    search_line: dict
    search_response: int

class AdvertismentStartEvent(UserAction):
    ad_id: UUID4

class AdvertismentFinishEvent(UserAction):
    ad_id: UUID4


