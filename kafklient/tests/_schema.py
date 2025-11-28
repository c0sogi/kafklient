from dataclasses import dataclass


@dataclass(frozen=True)
class HelloRecord:
    message: str
    count: int


@dataclass(frozen=True)
class IdxRecord:
    idx: int


@dataclass(frozen=True)
class FlagRecord:
    test: bool


@dataclass(frozen=True)
class EchoPayload:
    action: str
    value: int


@dataclass(frozen=True)
class RPCResponsePayload:
    status: str
    echo: EchoPayload
    server: str
