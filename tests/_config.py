from os import getenv

# Default to IPv4 loopback to avoid Windows preferring IPv6 (::1) for "localhost"
# which can fail when Docker publishes ports on IPv4 only (0.0.0.0).
KAFKA_BOOTSTRAP: str = getenv("KAFKLIENT_TEST_BOOTSTRAP", "127.0.0.1:9092")
TEST_TIMEOUT: float = float(getenv("KAFKLIENT_TEST_TIMEOUT", "10.0"))
