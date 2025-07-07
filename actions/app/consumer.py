import json
import logging
from abc import ABC, abstractmethod

from kafka import KafkaConsumer

from .models import AdEvent, TrackEvent

logger = logging.getLogger(__name__)


class MessageHandler(ABC):
    def __init__(self, topic_name: str) -> None:
        self.topic_name = topic_name

    @abstractmethod
    def handle(self, message: str) -> None: ...


class TrackHandler(MessageHandler):
    def handle(self, message: str) -> None:
        try:
            track_event = TrackEvent(**json.loads(message))
            print(f"Got track message with id {track_event.action_id}")
        except Exception as err:
            _msg = f"Problem with message strucure: {message} - {err}"
            logger.error(_msg)


class AdHandler(MessageHandler):
    def handle(self, message: str) -> None:
        try:
            ad_event = AdEvent(**json.loads(message))
            print(f"Got ad message with id {ad_event.action_id}")
        except Exception as err:
            _msg = f"Problem with message strucure: {message} - {err}"
            logger.error(_msg)


class ActionKafkaConsumer:
    def __init__(self, topics: list[str]) -> None:
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=["localhost:9094"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
        )
        self.handlers: dict[str, MessageHandler] = {}

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
                    self.handlers[message.topic].handle(message.value.decode("utf-8"))
        except Exception as err:
            _msg = f"Consumer having problems {err} and shutting down..."
            logger.error(_msg)
        finally:
            self.consumer.close()
