import asyncio
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
        self.batch: list[dict] = []
        self.batch_size = 1000
        self.batch_timeout = 5.0

    @abstractmethod
    async def handle(self, message: str, storage: Storage) -> None: ...

    async def flush_batch(self, storage: Storage) -> None:
        if self.batch:
            try:
                async with storage as db:
                    await db.insert_rows(self.batch, self.table_name)
                _msg = f"Inserted {len(self.batch)} rows to {self.table_name}"
                logger.debug(_msg)
                self.batch = []
            except Exception as err:
                _msg = f"Error inserting batch to {self.table_name}: {err}"
                logger.error(_msg)


class TrackHandler(MessageHandler):
    async def handle(self, message: str, storage: Storage) -> None:
        try:
            track_event = TrackEvent(**json.loads(message))
            self.batch.append(track_event.model_dump(mode="json"))

            if len(self.batch) >= self.batch_size:
                await self.flush_batch(storage)

        except Exception as err:
            _msg = f"Problem with message strucure: {message} - {err}"
            logger.error(_msg)


class AdHandler(MessageHandler):
    async def handle(self, message: str, storage: Storage) -> None:
        try:
            ad_event = AdEvent(**json.loads(message))
            self.batch.append(ad_event.model_dump(mode="json"))

            if len(self.batch) >= self.batch_size:
                await self.flush_batch(storage)

        except Exception as err:
            _msg = f"Problem with message strucure: {message} - {err}"
            logger.error(_msg)


class ActionProcessor:
    def __init__(self, consumer: AIOKafkaConsumer, storage: Storage) -> None:
        self.consumer = consumer
        self.handlers: dict[str, MessageHandler] = {}
        self.storage = storage
        self._running = False
        self._flush_task = None
        self._flush_period = 3

    async def register_handler(self, handler: MessageHandler) -> None:
        try:
            self.handlers[handler.topic_name] = handler
            self.consumer.subscribe(list(self.handlers.keys()))
            _msg = f"Successfully registered handler for topic {handler.topic_name}"
            logger.info(_msg)
        except Exception as err:
            _msg = f"Could not register handler {handler} for topic {handler.topic_name} : {err}"
            logger.error(_msg, exc_info=True)
            raise

    async def _periodic_flush(self):
        while self._running:
            try:
                for handler in self.handlers.values():
                    await handler.flush_batch(self.storage)
                await asyncio.sleep(self._flush_period)
            except Exception as err:
                _msg = f"Error during periodic flush: {err}"
                logger.error(_msg)

    async def run(self) -> None:
        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())

        try:
            await self.consumer.start()
            async for message in self.consumer:
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
            if self._flush_task:
                await self._flush_task

            for handler in self.handlers.values():
                await handler.flush_batch(self.storage)

            await self.consumer.stop()
            logger.info("Consumer stopped successfully")

    async def stop(self) -> None:
        self._running = False
        await self.consumer.stop()
