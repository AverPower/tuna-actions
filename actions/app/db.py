from clickhouse_driver import Client

class ClickHouseDB:
    def __init__(self, host: str) -> None:
        self.client = Client(host=host)

    def start(self):
        print(self.client.execute("SHOW DATABASES"))


if __name__ == "__main__":
    db = ClickHouseDB(host="localhost")
