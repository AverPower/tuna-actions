from kafka import KafkaConsumer


class ActionKafkaConsumer:
    def __init__(self, topics: list[str]) -> None:
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=["localhost:9094"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
        )

    def run(self) -> None:
        try:
            for message in self.consumer:
                with open("test.file", "a") as fout:
                    fout.write(message.value.decode("utf-8") + "\n")
        except Exception as err:
            with open("test.file", "a") as fout:
                fout.write(str(err))
