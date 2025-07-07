from abc import ABC, abstractmethod
from kafka import KafkaConsumer


class MessageHandler(ABC):

    def __init__(self, topic_name: str) -> None:
        self.topic_name = topic_name

    @abstractmethod
    def handle(self, message: str) -> None:
        pass

class TrackHandler(MessageHandler):

    def handle(self, message: str) -> None:
        ...

class AdHandler(MessageHandler):

    def handle(self, message: str) -> None:
        ...


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
        self.handlers[handler.topic_name] = handler
        self.consumer.subscribe(list(self.handlers.keys()))

    def run(self) -> None:
        try:
            for message in self.consumer:
                if message.topic in self.handlers:
                    self.handlers[message.topic].handle(message.value.decode("utf-8"))
        except Exception as err:
            ...
        finally:
            self.consumer.close()
