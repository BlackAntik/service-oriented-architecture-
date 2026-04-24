import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_NAME = os.getenv("TOPIC_NAME", "movie-events")
SCHEMA_FILE = os.getenv("SCHEMA_FILE", "/schemas/movie_event.avsc")
GENERATOR_INTERVAL_SEC = float(os.getenv("GENERATOR_INTERVAL_SEC", "0.5"))
GENERATOR_USERS = int(os.getenv("GENERATOR_USERS", "10"))
