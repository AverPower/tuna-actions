import json
import logging
from abc import ABC, abstractmethod

from aiokafka import AIOKafkaConsumer
from models import AdEvent, TrackEvent
from storage import Storage

logger = logging.getLogger(__name__)


class MessageHandler(ABC):
    def __init__(self, topic_name: str, table_name: str) -> None:
        self.topic_name = topic_name
        self.table_name = table_name

    @abstractmethod
    async def handle(self, message: str, storage: Storage) -> None: ...


class TrackHandler(MessageHandler):
    async def handle(self, message: str, storage: Storage) -> None:
        try:
            track_event = TrackEvent(**json.loads(message))
            _msg = f"Got track message with id {track_event.action_id}"
            logger.debug(_msg)
            async with storage as db:
                await db.insert_row(track_event.model_dump(), self.table_name)
        except Exception as err:
            _msg = f"Problem with message strucure: {message} - {err}"
            logger.error(_msg)


class AdHandler(MessageHandler):
    async def handle(self, message: str, storage: Storage) -> None:
        try:
            ad_event = AdEvent(**json.loads(message))
            _msg = f"Got ad message with id {ad_event.action_id}"
            logger.debug(_msg)
            async with storage as db:
                await db.insert_row(ad_event.model_dump(), self.table_name)
        except Exception as err:
            _msg = f"Problem with message strucure: {message} - {err}"
            logger.error(_msg)


class ActionProcessor:
    def __init__(self, consumer: AIOKafkaConsumer, storage: Storage) -> None:
        self.consumer = consumer
        self.handlers: dict[str, MessageHandler] = {}
        self.storage = storage
        self.running = False

    async def register_handler(self, handler: MessageHandler) -> None:
        try:
            self.handlers[handler.topic_name] = handler
            await self.consumer.subscribe(list(self.handlers.keys()))
            _msg = f"Successfully registered handler for topic {handler.topic_name}"
            logger.info(_msg)
        except Exception as err:
            _msg = f"Could not register handler {handler} for topic {handler.topic_name} : {err}"
            logger.error(_msg, exc_info=True)
            raise

    async def run(self) -> None:
        self.running = True
        try:
            for message in self.consumer:
                if not self._running:
                    break
                if message.topic in self.handlers:
                    try:
                        await self.handlers[message.topic].handle(
                            message.value.decode("utf-8"), self.storage
                        )
                    except Exception as handler_err:
                        _msg = f"Error processing message from topic {message.topic}: {handler_err}"
                        logger.error(_msg, exc_info=True)
        except Exception as err:
            _msg = f"Consumer having problems {err} and shutting down..."
            logger.error(_msg)
        finally:
            self._running = False
            await self.consumer.stop()
            logger.info("Consumer stopped successfully")

    async def stop(self) -> None:
        self._running = False
        await self.consumer.stop()
