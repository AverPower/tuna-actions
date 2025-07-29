import logging
from argparse import ArgumentParser

from kafka import KafkaAdminClient
from kafka.admin import NewTopic

logger = logging.getLogger(__name__)


DEFAULT_NUM_PARTITIONS = 3
DEFAULT_REPLICATION_FACTOR = 3
DEFAULT_TOPIC_CONFIG = {
    "retention.ms": "43200000",
    "cleanup.policy": "delete"
}

def add_topics(servers: list[str], topic_names: list[str]) -> None:
    admin_client = KafkaAdminClient(
        bootstrap_servers=servers,
        client_id="my_admin"
    )
    existing_topics = set(admin_client.list_topics())
    new_topics_names = set(topic_names).difference(existing_topics)
    new_topics = []
    for new_topic_name in new_topics_names:
        new_topic = NewTopic(
            name=new_topic_name,
            num_partitions=DEFAULT_NUM_PARTITIONS,
            replication_factor=DEFAULT_REPLICATION_FACTOR,
            topic_configs=DEFAULT_TOPIC_CONFIG
        )
        new_topics.append(new_topic)
    try:
        admin_client.create_topics(new_topics)
    except Exception as err:
        _msg = f"Error with creating topics {topic_names}: {err}"
        logger.error(_msg)
    finally:
        admin_client.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-t", "--topic_names", nargs="+")
    args = parser.parse_args()
    add_topics(args.topic_names)
