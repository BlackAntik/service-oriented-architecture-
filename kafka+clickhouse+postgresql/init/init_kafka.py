import json
import os
import time
import requests
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ConfigSource
from confluent_kafka import KafkaException


SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "movie-events")
SCHEMA_FILE = os.getenv("SCHEMA_FILE", "/schemas/movie_event.avsc")
TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
TOPIC_REPLICATION_FACTOR = int(os.getenv("TOPIC_REPLICATION_FACTOR", "2"))
TOPIC_MIN_ISR = int(os.getenv("TOPIC_MIN_ISR", "1"))


def wait_for_service(url: str, timeout: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(3)
    raise TimeoutError(f"Service {url} not available after {timeout}s")


def wait_for_kafka(bootstrap_servers: str, timeout: int = 120) -> None:
    """Wait until all brokers in bootstrap_servers are reachable."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    start = time.time()
    while time.time() - start < timeout:
        try:
            meta = admin.list_topics(timeout=5)
            brokers = meta.brokers
            if len(brokers) >= 2:
                print(f"Kafka cluster ready: {len(brokers)} brokers")
                return
            print(f"Waiting for Kafka cluster: {len(brokers)} broker(s) found, need 2...")
        except KafkaException as exc:
            print(f"Kafka not ready yet: {exc}")
        time.sleep(3)
    raise TimeoutError(f"Kafka cluster not ready after {timeout}s")


def register_schema(schema_registry_url: str, subject: str, schema_path: str) -> int:
    with open(schema_path, "r") as f:
        schema_str = json.dumps(json.load(f))

    payload = {"schema": schema_str, "schemaType": "AVRO"}
    response = requests.post(
        f"{schema_registry_url}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    schema_id = response.json()["id"]
    return schema_id


def create_topic(
    bootstrap_servers: str,
    topic_name: str,
    num_partitions: int = 3,
    replication_factor: int = 2,
    min_insync_replicas: int = 1,
) -> None:
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin_client.list_topics(timeout=10).topics
    if topic_name in existing:
        print(f"Topic '{topic_name}' already exists, skipping creation")
        return

    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        config={
            "min.insync.replicas": str(min_insync_replicas),
        },
    )
    futures = admin_client.create_topics([new_topic])
    for topic, future in futures.items():
        future.result()
    print(
        f"Topic '{topic_name}' created: partitions={num_partitions}, "
        f"replication_factor={replication_factor}, min.insync.replicas={min_insync_replicas}"
    )


def verify_topic(bootstrap_servers: str, topic_name: str) -> None:
    """Print topic metadata to verify replication."""
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    meta = admin_client.list_topics(timeout=10)
    topic_meta = meta.topics.get(topic_name)
    if topic_meta is None:
        print(f"WARNING: topic '{topic_name}' not found in metadata")
        return
    for pid, partition in topic_meta.partitions.items():
        print(
            f"  Partition {pid}: leader={partition.leader}, "
            f"replicas={list(partition.replicas)}, "
            f"isrs={list(partition.isrs)}"
        )


def main() -> None:
    # Wait for Schema Registry (which implies Kafka is up)
    wait_for_service(f"{SCHEMA_REGISTRY_URL}/subjects")

    # Additionally wait for both Kafka brokers
    wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    schema_id = register_schema(
        schema_registry_url=SCHEMA_REGISTRY_URL,
        subject=f"{TOPIC_NAME}-value",
        schema_path=SCHEMA_FILE,
    )
    print(f"Schema registered with id={schema_id}")

    create_topic(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic_name=TOPIC_NAME,
        num_partitions=TOPIC_PARTITIONS,
        replication_factor=TOPIC_REPLICATION_FACTOR,
        min_insync_replicas=TOPIC_MIN_ISR,
    )

    print(f"\nTopic '{TOPIC_NAME}' partition details:")
    verify_topic(KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME)


if __name__ == "__main__":
    main()
