from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Coroutine, Sequence

from di.dependent import Dependent
from di.executors import AsyncExecutor
from pydantic import BaseModel

from kaflow.dependencies import Scopes
from kaflow.exceptions import KaflowDeserializationException
from kaflow.message import Message, ReadMessage

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord
    from di import Container, ScopeState

    from kaflow.applications import ConsumerFunc, DeserializationErrorHandlerFunc
    from kaflow.serializers import Serializer
    from kaflow.typing import TopicValueKeyHeader


def _deserialize(
    value: bytes,
    param_type: type[TopicValueKeyHeader] | None,
    deserializer: Serializer | None,
) -> TopicValueKeyHeader:
    if deserializer:
        deserialized: dict[str, Any] = deserializer.deserialize(value)
        if (
            param_type is not None
            and hasattr(param_type, "__bases__")
            and BaseModel in param_type.__bases__
        ):
            return param_type(**deserialized)
        return deserialized
    return value


class TopicConsumerFunc:
    __slots__ = (
        "name",
        "container",
        "publish_fn",
        "exception_handlers",
        "deserialization_error_handler",
        "dependent",
        "func",
        "value_param_type",
        "value_deserializer",
        "key_param_type",
        "key_deserializer",
        "headers_type_deserializers",
        "sink_topics",
        "executor",
        "container_state",
    )

    def __init__(
        self,
        *,
        name: str,
        container: Container,
        publish_fn: Callable[
            [
                str,
                bytes | None,
                bytes | None,
                dict[str, bytes] | None,
                int | None,
                int | None,
            ],
            Coroutine[Any, Any, None],
        ],
        exception_handlers: dict[type[Exception], Callable[..., Awaitable[None]]],
        deserialization_error_handler: DeserializationErrorHandlerFunc | None = None,
        func: ConsumerFunc,
        value_param_type: type[TopicValueKeyHeader],
        value_deserializer: Serializer | None = None,
        key_param_type: type[TopicValueKeyHeader] | None = None,
        key_deserializer: Serializer | None = None,
        headers_type_deserializers: dict[
            str, tuple[type[TopicValueKeyHeader], Serializer | None]
        ]
        | None = None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        self.name = name
        self.container = container
        self.publish_fn = publish_fn
        self.exception_handlers = exception_handlers
        self.deserialization_error_handler = deserialization_error_handler
        self.dependent = self.container.solve(
            Dependent(func, scope="consumer"), scopes=Scopes
        )
        self.value_param_type = value_param_type
        self.value_deserializer = value_deserializer
        self.key_param_type = key_param_type
        self.key_deserializer = key_deserializer
        self.headers_type_deserializers = headers_type_deserializers
        self.sink_topics = sink_topics
        self.executor = AsyncExecutor()

    def prepare(self, state: ScopeState) -> None:
        self.container_state = state

    def _deserialize_value(self, value: bytes) -> TopicValueKeyHeader:
        return _deserialize(value, self.value_param_type, self.value_deserializer)

    def _deserialize_key(self, key: bytes) -> TopicValueKeyHeader | None:
        if self.key_param_type:
            return _deserialize(key, self.key_param_type, self.key_deserializer)
        return None

    def _deserialize_headers(
        self, headers: Sequence[tuple[str, bytes]]
    ) -> dict[str, Any] | None:
        if self.headers_type_deserializers:
            headers_ = {}
            for key, value in headers:
                header_type, deserializer = self.headers_type_deserializers.get(
                    key, (None, None)
                )
                headers_[key] = _deserialize(value, header_type, deserializer)
            return headers_
        return None

    async def _deserialize(
        self, record: ConsumerRecord
    ) -> tuple[
        TopicValueKeyHeader | None,
        TopicValueKeyHeader | None,
        dict[str, TopicValueKeyHeader] | None,
        bool,
    ]:
        async def handle_deserialization_error(
            error_message: str, record: ConsumerRecord, exception: Exception
        ) -> None:
            exc = KaflowDeserializationException(
                error_message.format(self.name), record=record
            )
            if not self.deserialization_error_handler:
                raise exc from exception
            await self.deserialization_error_handler(exc)

        deserialized = True
        try:
            value = self._deserialize_value(record.value)
        except Exception as e:
            await handle_deserialization_error(
                (
                    "Failed to deserialize value of message comming from topic"
                    f" `{self.name}`"
                ),
                record,
                e,
            )
            value = None
            deserialized = False

        try:
            key = self._deserialize_key(record.key)
        except Exception as e:
            await handle_deserialization_error(
                (
                    "Failed to deserialize key of message comming from topic"
                    f" `{self.name}`"
                ),
                record,
                e,
            )
            key = None
            deserialized = False

        try:
            headers = self._deserialize_headers(record.headers)
        except Exception as e:
            await handle_deserialization_error(
                (
                    "Failed to deserialize headers of message comming from topic"
                    f" `{self.name}`"
                ),
                record,
                e,
            )
            headers = None
            deserialized

        return value, key, headers, deserialized

    def _lookup_exception_handler(
        self, exc: Exception
    ) -> Callable[..., Awaitable[None]] | None:
        for cls in type(exc).__mro__:
            if cls in self.exception_handlers:
                return self.exception_handlers[cls]
        return None

    async def _execute_dependent(
        self,
        consumer_state: ScopeState,
        message: ReadMessage,
    ) -> Any:
        try:
            return await self.dependent.execute_async(
                executor=self.executor,
                state=consumer_state,
                values={ReadMessage: message},
            )
        except tuple(self.exception_handlers.keys()) as e:
            handler = self._lookup_exception_handler(e)
            if not handler:
                raise e
            await handler(e)
            return None

    async def _publish_messages(self, message: Message) -> None:
        if self.sink_topics:
            await asyncio.gather(
                *[
                    self.publish_fn(
                        topic,
                        message.value,
                        message.key,
                        message.headers,
                        message.partition,
                        message.offset,
                    )
                    for topic in self.sink_topics
                ]
            )

    async def _process(self, read_message: ReadMessage) -> None:
        async with self.container.enter_scope(
            "consumer", state=self.container_state
        ) as consumer_state:
            message = await self._execute_dependent(
                consumer_state=consumer_state, message=read_message
            )
        if message and isinstance(message, Message):
            await self._publish_messages(message)

    async def consume(self, record: ConsumerRecord) -> None:
        value, key, headers, deserialized = await self._deserialize(record)
        if not deserialized:
            return
        message = ReadMessage(
            value=value,
            key=key,
            headers=headers,
            offset=record.offset,
            partition=record.partition,
            timestamp=record.timestamp,
        )
        await self._process(read_message=message)
