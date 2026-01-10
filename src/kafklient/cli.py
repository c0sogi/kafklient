import asyncio
import logging
import os
import sys
from typing import TypeGuard

import typer

app = typer.Typer(no_args_is_help=True)


@app.command()
def mcp_client(
    bootstrap_servers: str = typer.Option(
        "127.0.0.1:9092",
        "--bootstrap-servers",
        "-b",
        envvar="KAFKLIENT_MCP_BOOTSTRAP",
        help="Kafka bootstrap servers",
        show_default=True,
    ),
    consumer_topic: str | None = typer.Option(
        None,
        "--consumer-topic",
        "-c",
        help=(
            "Kafka topic to read responses/notifications from. "
            "If omitted, uses $KAFKLIENT_MCP_CONSUMER_TOPIC or 'mcp-responses'. "
            "Messages are filtered by the 'x-session-id' header for session isolation."
        ),
    ),
    producer_topic: str = typer.Option(
        "mcp-requests",
        "--producer-topic",
        "-p",
        envvar="KAFKLIENT_MCP_PRODUCER_TOPIC",
        help="Kafka topic to write requests to",
        show_default=True,
    ),
    consumer_group_id: str | None = typer.Option(
        None,
        "--consumer-group-id",
        "-g",
        envvar="KAFKLIENT_MCP_CONSUMER_GROUP_ID",
        help="Kafka consumer group id for the response consumer (default: auto-generated)",
        show_default=False,
    ),
    consumer_config: list[str] = typer.Option(
        [],
        "--consumer-config",
        "-C",
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
        "-P",
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
        "-l",
        envvar="KAFKLIENT_MCP_LOG_LEVEL",
        help="Logging level",
        show_default=True,
    ),
) -> None:
    """Run the MCP stdio-to-Kafka bridge (client side)."""
    from kafklient.mcp._utils import parse_kafka_config
    from kafklient.mcp.client import run_client_async

    # MCP stdio transport requires stdout to remain protocol-only.
    # Force Python logging to stderr, even if something configured logging earlier.
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        stream=sys.stderr,
        force=True,
    )

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
        )
    )


@app.command(no_args_is_help=True)
def mcp_server(
    mcp: str = typer.Argument(
        ...,
        help=("FastMCP object spec. e.g. mypkg.myserver:mcp or ./myserver.py:mcp (':' is optional)"),
    ),
    bootstrap_servers: str = typer.Option(
        "127.0.0.1:9092",
        "--bootstrap-servers",
        "-b",
        envvar="KAFKLIENT_MCP_BOOTSTRAP",
        help="Kafka bootstrap servers",
        show_default=True,
    ),
    consumer_topic: str = typer.Option(
        "mcp-requests",
        "--consumer-topic",
        "-c",
        help="Kafka topic to read requests from",
        show_default=True,
    ),
    producer_topic: str = typer.Option(
        "mcp-responses",
        "--producer-topic",
        "-p",
        help="Kafka topic to write responses/notifications to",
        show_default=True,
    ),
    consumer_group_id: str | None = typer.Option(
        None,
        "--consumer-group-id",
        "-g",
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
        "-t",
        help="Consumer assignment timeout seconds",
        show_default=True,
    ),
    consumer_config: list[str] = typer.Option(
        [],
        "--consumer-config",
        "-C",
        metavar="KEY=VALUE",
        help="Extra consumer config entries (repeatable).",
        show_default=False,
    ),
    producer_config: list[str] = typer.Option(
        [],
        "--producer-config",
        "-P",
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
        "-l",
        help="Log level for the server (overrides default temporarily)",
        show_default=False,
    ),
) -> None:
    """Run a FastMCP server over Kafka (stdio transport bridged to Kafka)."""

    from kafklient.mcp._utils import load_object_from_spec, parse_kafka_config
    from kafklient.mcp.server import Server, run_server

    def load_mcp_object_from_spec(
        spec: str,
        *,
        default_object_name: str | None,
        param_hint: str | None,
    ) -> Server:
        def is_server_instance(obj: object) -> TypeGuard[Server]:
            # `Server` is a typing union; `isinstance(x, Server)` is invalid at runtime.
            try:
                from fastmcp import FastMCP as ExternalFastMCP
                from mcp.server import FastMCP as McpFastMCP
            except Exception:
                return False
            return isinstance(obj, (McpFastMCP, ExternalFastMCP))

        maybe_mcp: object = load_object_from_spec(
            spec,
            default_object_name=default_object_name,
            param_hint=param_hint,
        )
        if is_server_instance(maybe_mcp):
            return maybe_mcp
        elif callable(maybe_mcp):
            try:
                maybe_mcp = maybe_mcp()
            except Exception as e:
                raise typer.BadParameter(f"Failed to call MCP factory: {e}", param_hint=param_hint) from e
            if not is_server_instance(maybe_mcp):
                raise typer.BadParameter("Factory result is not a FastMCP or MCP instance.", param_hint=param_hint)
            return maybe_mcp
        else:
            raise typer.BadParameter("Must be a FastMCP instance (or a zero-arg factory).", param_hint=param_hint)

    # Match mcp_client behavior: honor CLI log level for Python logging too.
    # (FastMCP logging may still be adjusted separately via temporary_log_level in run_server.)
    base_level_name = (log_level or "INFO").upper()
    # Keep stdout clean when used under stdio-based transports.
    logging.basicConfig(
        level=getattr(logging, base_level_name, logging.INFO),
        stream=sys.stderr,
        force=True,
    )
    parsed_consumer_config, parsed_producer_config = parse_kafka_config(
        consumer_config=consumer_config,
        producer_config=producer_config,
        consumer_config_json=consumer_config_json,
        producer_config_json=producer_config_json,
        default_consumer_config={"auto.offset.reset": "latest"},
        default_producer_config={},
    )
    run_server(
        mcp=load_mcp_object_from_spec(mcp, default_object_name="mcp", param_hint="--mcp"),
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
    )
