import json
import logging
from abc import ABC, abstractmethod

from kafka import KafkaConsumer

from .models import AdEvent, TrackEvent
from .storage import Storage

logger = logging.getLogger(__name__)


class MessageHandler(ABC):
    def __init__(self, topic_name: str, table_name: str) -> None:
        self.topic_name = topic_name
        self.table_name = table_name

    @abstractmethod
    def handle(self, message: str, storage: Storage) -> None: ...


class TrackHandler(MessageHandler):
    def handle(self, message: str, storage: Storage) -> None:
        try:
            track_event = TrackEvent(**json.loads(message))
            _msg = f"Got track message with id {track_event.action_id}"
            logger.debug(_msg)
            storage.insert_row(track_event.model_dump(), self.table_name)
        except Exception as err:
            _msg = f"Problem with message strucure: {message} - {err}"
            logger.error(_msg)


class AdHandler(MessageHandler):
    def handle(self, message: str, storage: Storage) -> None:
        try:
            ad_event = AdEvent(**json.loads(message))
            _msg = f"Got ad message with id {ad_event.action_id}"
            logger.debug(_msg)
            storage.insert_row(ad_event.model_dump(), self.table_name)
        except Exception as err:
            _msg = f"Problem with message strucure: {message} - {err}"
            logger.error(_msg)


class ActionProcessor:
    def __init__(self, consumer: KafkaConsumer, storage: Storage) -> None:
        self.consumer = consumer
        self.handlers: dict[str, MessageHandler] = {}
        self.storage = storage

    def register_handler(self, handler: MessageHandler) -> None:
        try:
            self.handlers[handler.topic_name] = handler
            self.consumer.subscribe(list(self.handlers.keys()))
        except Exception as err:
            _msg = f"Could not register handler {handler} for topic {handler.topic_name} : {err}"
            logger.error(_msg)

    def run(self) -> None:
        try:
            for message in self.consumer:
                if message.topic in self.handlers:
                    self.handlers[message.topic].handle(
                        message.value.decode("utf-8"), self.storage
                    )
        except Exception as err:
            _msg = f"Consumer having problems {err} and shutting down..."
            logger.error(_msg)
        finally:
            self.consumer.close()
