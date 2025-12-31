import importlib
import json
import runpy
from pathlib import Path

import typer
from pydantic import TypeAdapter

from kafklient.types.backend import Message as KafkaMessage
from kafklient.types.config import ConsumerConfig, ProducerConfig

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


def load_object_from_spec(
    spec: str,
    *,
    default_object_name: str | None,
    param_hint: str | None,
) -> object:
    """
    spec formats:
    - "some.module:obj" (or "some.module:obj.attr")
    - "path/to/file.py:obj" (or "path/to/file.py:obj.attr")

    If ":" is omitted, we assume ":{default_object_name}".
    """
    raw = spec.strip()
    if not raw:
        raise typer.BadParameter("Must not be empty.", param_hint=param_hint)

    if ":" in raw:
        target_raw, obj_path_raw = raw.split(":", 1)
    elif default_object_name:
        target_raw, obj_path_raw = raw, default_object_name
    else:
        raise typer.BadParameter("Must specify a module name or file path.", param_hint=param_hint)

    target = target_raw.strip()
    obj_path = obj_path_raw.strip()
    if not target:
        raise typer.BadParameter("Module name or file path is required.", param_hint=param_hint)
    if not obj_path:
        raise typer.BadParameter("Object path is required (e.g. module:obj).", param_hint=param_hint)

    attrs = [p for p in obj_path.split(".") if p]
    if not attrs:
        raise typer.BadParameter("Invalid object path.", param_hint=param_hint)

    # File path mode
    target_path = Path(target)
    if target_path.suffix.lower() == ".py" or target_path.exists():
        try:
            ns = runpy.run_path(str(target_path))
        except Exception as e:
            raise typer.BadParameter(f"Failed to load file: {e}", param_hint=param_hint) from e

        if attrs[0] not in ns:
            raise typer.BadParameter(
                f"Object {attrs[0]!r} not found in file.",
                param_hint=param_hint,
            )
        obj: object = ns[attrs[0]]
        for attr in attrs[1:]:
            try:
                obj = getattr(obj, attr)
            except Exception as e:
                raise typer.BadParameter(f"Failed to access attribute {attr!r}: {e}", param_hint=param_hint) from e
        return obj

    # Module mode
    try:
        mod = importlib.import_module(target)
    except Exception as e:
        raise typer.BadParameter(f"Failed to import module: {e}", param_hint=param_hint) from e

    try:
        obj = getattr(mod, attrs[0])
    except Exception as e:
        raise typer.BadParameter(f"Object {attrs[0]!r} not found in module: {e}", param_hint=param_hint) from e

    for attr in attrs[1:]:
        try:
            obj = getattr(obj, attr)
        except Exception as e:
            raise typer.BadParameter(f"Failed to access attribute {attr!r}: {e}", param_hint=param_hint) from e
    return obj


def parse_kafka_config(
    consumer_config: list[str],
    producer_config: list[str],
    consumer_config_json: str | None,
    producer_config_json: str | None,
    *,
    default_consumer_config: ConsumerConfig,
    default_producer_config: ProducerConfig,
) -> tuple[ConsumerConfig, ProducerConfig]:
    def _parse_kv_items(items: list[str]) -> dict[str, object]:
        def _parse_value(raw: str) -> object:
            lowered = raw.strip().lower()
            if lowered in {"true", "false"}:
                return lowered == "true"
            if lowered in {"null", "none"}:
                return None

            # Try int/float
            try:
                if "." in raw:
                    return float(raw)
                return int(raw)
            except ValueError:
                pass

            # Try JSON (dict/list/strings/numbers)
            if raw and raw[0] in {"{", "[", '"'}:
                try:
                    return json.loads(raw)
                except Exception:
                    pass

            return raw

        out: dict[str, object] = {}
        for item in items:
            if "=" not in item:
                raise ValueError(f"Invalid config item {item!r}. Expected KEY=VALUE.")
            k, v = item.split("=", 1)
            k = k.strip()
            if not k:
                raise ValueError(f"Invalid config item {item!r}. Key cannot be empty.")
            out[k] = _parse_value(v.strip())
        return out

    if consumer_config_json:
        final_consumer_config = default_consumer_config | TypeAdapter(ConsumerConfig).validate_json(
            consumer_config_json
        )
    elif consumer_config:
        final_consumer_config = default_consumer_config | TypeAdapter(ConsumerConfig).validate_python(
            _parse_kv_items(consumer_config)
        )
    else:
        final_consumer_config = default_consumer_config

    if producer_config_json:
        final_producer_config = default_producer_config | TypeAdapter(ProducerConfig).validate_json(
            producer_config_json
        )
    elif producer_config:
        final_producer_config = default_producer_config | TypeAdapter(ProducerConfig).validate_python(
            _parse_kv_items(producer_config)
        )
    else:
        final_producer_config = default_producer_config

    return final_consumer_config, final_producer_config
