from functools import cached_property
from inspect import isawaitable
from typing import Awaitable, Callable, Generic, Optional, Type, TypeVar

from pydantic import BaseModel, ConfigDict, TypeAdapter

from .backend import Message

T = TypeVar("T")
T_Co = TypeVar("T_Co", covariant=True)

Factory = Callable[[Message], T | Awaitable[T]]
CorrelationCallback = Callable[[Message, object], Optional[bytes] | Awaitable[Optional[bytes]]]


class Parser(BaseModel, Generic[T_Co]):
    topics: list[str]
    factory: Optional[Factory[T_Co]] = None

    @cached_property
    def default_parser(self) -> Factory[T_Co]:
        if self.type is Message:
            return lambda record: record  # pyright: ignore[reportReturnType]
        adapter: TypeAdapter[T_Co] = make_adapter(self.type)

        # NOTE:
        # Kafka record.value() is typically bytes. For Pydantic models (and many other types),
        # JSON bytes should be validated via validate_json(), not validate_python(bytes).
        # To keep backward compatibility for binary payloads, we try validate_json() first
        # (only when the expected output type is not "bytes-like"), then fall back to validate_python().
        def _parse(record: Message) -> T_Co:
            raw: bytes = record.value() or b""

            # If the user expects raw bytes, do not try to interpret them as JSON.
            if self.type is not bytes:
                try:
                    return adapter.validate_json(raw)
                except Exception:
                    pass
            return adapter.validate_python(raw)

        return _parse

    @cached_property
    def type(self) -> Type[T_Co]:
        args = type(self).__pydantic_generic_metadata__["args"]
        if args:
            return args[0]
        return object  # pyright: ignore[reportReturnType]

    async def aparse(self, record: Message) -> T_Co:
        if isawaitable(parsed := (self.factory or self.default_parser)(record)):
            return await parsed
        else:
            return parsed

    def parse(self, record: Message) -> T_Co:
        if isawaitable(parsed := (self.factory or self.default_parser)(record)):
            raise ValueError(
                f"{self!s} returned an awaitable; async parsers are not supported in sync mode. Use `aparse` instead."
            )
        return parsed

    def __str__(self) -> str:
        return f"Parser(topics={self.topics}, type={self.type.__name__})"


def make_adapter(tp: Type[T]) -> TypeAdapter[T]:
    if isinstance(tp, type) and issubclass(tp, BaseModel):  # pyright: ignore[reportUnnecessaryIsInstance]
        return TypeAdapter[T](tp)
    return TypeAdapter[T](tp, config=ConfigDict(arbitrary_types_allowed=True))


if __name__ == "__main__":
    parser = Parser(topics=["test"], factory=lambda record: int(record.value() or b""))
    print(parser.type)
