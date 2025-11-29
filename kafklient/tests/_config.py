from os import getenv

KAFKA_BOOTSTRAP: str = getenv("KAFKLIENT_TEST_BOOTSTRAP", "localhost:9092")
TEST_TIMEOUT: float = float(getenv("KAFKLIENT_TEST_TIMEOUT", "10.0"))
