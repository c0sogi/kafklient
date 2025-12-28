from kafklient.types.backend import Message as KafkaMessage

REPLY_TOPIC_HEADER_KEY = "x-reply-topic"
SESSION_ID_HEADER_KEY = "x-session-id"


def extract_header_bytes(record: KafkaMessage, header_key: str) -> bytes | None:
    try:
        headers = record.headers() or []
    except Exception:
        headers = []
    for k, v in headers:
        if k.lower() != header_key.lower():
            continue
        if v is None:  # pyright: ignore[reportUnnecessaryComparison]
            return None
        try:
            if isinstance(v, bytes):  # pyright: ignore[reportUnnecessaryIsInstance]
                return v
            return str(v).encode("utf-8")
        except Exception:
            continue
    return None


def extract_session_id(record: KafkaMessage) -> bytes | None:
    try:
        headers = record.headers() or []
    except Exception:
        headers = []
    for k, v in headers:
        if k.lower() != SESSION_ID_HEADER_KEY.lower():
            continue
        if v is None:  # pyright: ignore[reportUnnecessaryComparison]
            return None
        if isinstance(v, bytes):  # pyright: ignore[reportUnnecessaryIsInstance]
            return v
        return str(v).encode("utf-8")
    return None
