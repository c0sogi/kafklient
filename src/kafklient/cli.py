import asyncio
import importlib
import json
import logging
import os
import runpy
from pathlib import Path
from typing import TypeGuard

import typer
from pydantic import TypeAdapter

from kafklient.types import ConsumerConfig, ProducerConfig

app = typer.Typer(no_args_is_help=True)


@app.command(no_args_is_help=True)
def mcp_client(
    bootstrap_servers: str = typer.Option(
        "localhost:9092",
        "--bootstrap-servers",
        envvar="KAFKLIENT_MCP_BOOTSTRAP",
        help="Kafka bootstrap servers",
        show_default=True,
    ),
    consumer_topic: str | None = typer.Option(
        None,
        "--consumer-topic",
        help=(
            "Kafka topic to read responses/notifications from. "
            "If omitted, uses $KAFKLIENT_MCP_CONSUMER_TOPIC or 'mcp-responses'. "
            "When session isolation is enabled, messages are filtered by the 'x-session-id' header."
        ),
    ),
    producer_topic: str = typer.Option(
        "mcp-requests",
        "--producer-topic",
        envvar="KAFKLIENT_MCP_PRODUCER_TOPIC",
        help="Kafka topic to write requests to",
        show_default=True,
    ),
    consumer_group_id: str | None = typer.Option(
        None,
        "--consumer-group-id",
        envvar="KAFKLIENT_MCP_CONSUMER_GROUP_ID",
        help="Kafka consumer group id for the response consumer (default: auto-generated)",
        show_default=False,
    ),
    isolate_session: bool = typer.Option(
        os.getenv("KAFKLIENT_MCP_ISOLATE_SESSION", "true").strip().lower() not in {"0", "false", "no"},
        "--isolate-session/--no-isolate-session",
        help=(
            "Enable session isolation (default: true). "
            "When enabled and --consumer-topic is not provided, an instance-unique response topic is used."
        ),
        show_default=True,
    ),
    consumer_config: list[str] = typer.Option(
        [],
        "--consumer-config",
        metavar="KEY=VALUE",
        help=(
            "Extra consumer config entries (repeatable). "
            "Example: --consumer-config auto.offset.reset=latest "
            "--consumer-config enable.auto.commit=false"
        ),
        show_default=False,
    ),
    producer_config: list[str] = typer.Option(
        [],
        "--producer-config",
        metavar="KEY=VALUE",
        help="Extra producer config entries (repeatable). Example: --producer-config linger.ms=5",
        show_default=False,
    ),
    consumer_config_json: str | None = typer.Option(
        None,
        "--consumer-config-json",
        help="Extra consumer config as a JSON object string (merged after defaults).",
        show_default=False,
    ),
    producer_config_json: str | None = typer.Option(
        None,
        "--producer-config-json",
        help="Extra producer config as a JSON object string (merged after defaults).",
        show_default=False,
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        envvar="KAFKLIENT_MCP_LOG_LEVEL",
        help="Logging level",
        show_default=True,
    ),
) -> None:
    """Run the MCP stdio-to-Kafka bridge (client side)."""
    from kafklient.mcp.client import run_client_async

    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    parsed_consumer_config, parsed_producer_config = parse_kafka_config(
        consumer_config=consumer_config,
        producer_config=producer_config,
        consumer_config_json=consumer_config_json,
        producer_config_json=producer_config_json,
        default_consumer_config={"auto.offset.reset": "latest"},
        default_producer_config={},
    )
    asyncio.run(
        run_client_async(
            bootstrap_servers=bootstrap_servers,
            consumer_topic=(
                consumer_topic
                if consumer_topic is not None
                else os.getenv("KAFKLIENT_MCP_CONSUMER_TOPIC", "mcp-responses")
            ),
            producer_topic=producer_topic,
            consumer_group_id=consumer_group_id,
            consumer_config=parsed_consumer_config,
            producer_config=parsed_producer_config,
            isolate_session=isolate_session,
        )
    )


@app.command(no_args_is_help=True)
def mcp_server(
    mcp: str = typer.Option(
        ...,
        "--mcp",
        help=("FastMCP object spec. e.g. mypkg.myserver:mcp or ./myserver.py:mcp (':' is optional)"),
    ),
    bootstrap_servers: str = typer.Option(
        "localhost:9092",
        "--bootstrap-servers",
        envvar="KAFKLIENT_MCP_BOOTSTRAP",
        help="Kafka bootstrap servers",
        show_default=True,
    ),
    consumer_topic: str = typer.Option(
        "mcp-requests",
        "--consumer-topic",
        help="Kafka topic to read requests from",
        show_default=True,
    ),
    producer_topic: str = typer.Option(
        "mcp-responses",
        "--producer-topic",
        help="Kafka topic to write responses/notifications to",
        show_default=True,
    ),
    consumer_group_id: str | None = typer.Option(
        None,
        "--consumer-group-id",
        help="Kafka consumer group id for the request consumer (default: auto-generated)",
        show_default=False,
    ),
    auto_create_topics: bool = typer.Option(
        True,
        "--auto-create-topics/--no-auto-create-topics",
        help="Auto-create Kafka topics (best-effort)",
        show_default=True,
    ),
    assignment_timeout_s: float = typer.Option(
        5.0,
        "--assignment-timeout-s",
        help="Consumer assignment timeout seconds",
        show_default=True,
    ),
    consumer_config: list[str] = typer.Option(
        [],
        "--consumer-config",
        metavar="KEY=VALUE",
        help="Extra consumer config entries (repeatable).",
        show_default=False,
    ),
    producer_config: list[str] = typer.Option(
        [],
        "--producer-config",
        metavar="KEY=VALUE",
        help="Extra producer config entries (repeatable).",
        show_default=False,
    ),
    consumer_config_json: str | None = typer.Option(
        None,
        "--consumer-config-json",
        help="Extra consumer config as a JSON object string (merged after defaults).",
        show_default=False,
    ),
    producer_config_json: str | None = typer.Option(
        None,
        "--producer-config-json",
        help="Extra producer config as a JSON object string (merged after defaults).",
        show_default=False,
    ),
    show_banner: bool = typer.Option(
        True,
        "--show-banner/--no-show-banner",
        help="Display the server banner",
        show_default=True,
    ),
    log_level: str | None = typer.Option(
        None,
        "--log-level",
        help="Log level for the server (overrides default temporarily)",
        show_default=False,
    ),
    multi_session: bool = typer.Option(
        True,
        "--multi-session/--single-session",
        help="Enable multi-session (session isolation) mode",
        show_default=True,
    ),
) -> None:
    """Run a FastMCP server over Kafka (stdio transport bridged to Kafka)."""
    # Lazy imports to keep `kafklient` CLI usable without `kafklient[mcp]` unless needed.
    try:
        from fastmcp import FastMCP

        from kafklient.mcp.server import run_server
    except Exception as e:  # pragma: no cover
        raise typer.BadParameter(f"Failed to import MCP dependencies: {e}", param_hint="--mcp") from e

    logging.basicConfig(level=logging.INFO)

    loaded = _load_object_from_spec(mcp, default_object_name="mcp", param_hint="--mcp")

    def _is_fastmcp_like(obj: object) -> TypeGuard[FastMCP]:
        return isinstance(obj, FastMCP)

    mcp_obj: FastMCP
    if _is_fastmcp_like(loaded):
        mcp_obj = loaded
    elif callable(loaded):
        try:
            created = loaded()
        except Exception as e:
            raise typer.BadParameter(f"Failed to call MCP factory: {e}", param_hint="--mcp") from e
        if not _is_fastmcp_like(created):
            raise typer.BadParameter("Factory result is not a FastMCP instance.", param_hint="--mcp")
        mcp_obj = created
    else:
        raise typer.BadParameter("Must be a FastMCP instance (or a zero-arg factory).", param_hint="--mcp")

    parsed_consumer_config, parsed_producer_config = parse_kafka_config(
        consumer_config=consumer_config,
        producer_config=producer_config,
        consumer_config_json=consumer_config_json,
        producer_config_json=producer_config_json,
        default_consumer_config={"auto.offset.reset": "latest"},
        default_producer_config={},
    )
    run_server(
        mcp=mcp_obj,
        bootstrap_servers=bootstrap_servers,
        consumer_topic=consumer_topic,
        producer_topic=producer_topic,
        consumer_group_id=consumer_group_id,
        auto_create_topics=auto_create_topics,
        assignment_timeout_s=assignment_timeout_s,
        consumer_config=parsed_consumer_config,
        producer_config=parsed_producer_config,
        show_banner=show_banner,
        log_level=log_level,
        multi_session=multi_session,
    )


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


def _load_object_from_spec(
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
