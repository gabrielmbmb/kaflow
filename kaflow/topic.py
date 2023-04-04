from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Coroutine, Sequence

from di.dependent import Dependent
from di.executors import AsyncExecutor
from pydantic import BaseModel

from kaflow.dependencies import Scopes
from kaflow.exceptions import KaflowDeserializationException
from kaflow.serializers import _serialize

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord
    from di import Container, ScopeState

    from kaflow.serializers import Serializer
    from kaflow.typing import (
        ConsumerFunc,
        DeserializationErrorHandlerFunc,
        TopicMessage,
    )


class TopicProcessingFunc:
    __slots__ = (
        "name",
        "container",
        "publish_fn",
        "exception_handlers",
        "deserialization_error_handler",
        "dependent",
        "param_type",
        "deserializer",
        "return_type",
        "serializer",
        "sink_topics",
        "executor",
        "container_state",
    )

    def __init__(
        self,
        *,
        name: str,
        container: Container,
        publish_fn: Callable[[str, bytes], Coroutine[Any, Any, None]],
        exception_handlers: dict[type[Exception], Callable[..., Awaitable[None]]],
        deserialization_error_handler: DeserializationErrorHandlerFunc | None = None,
        func: ConsumerFunc | None = None,
        param_type: type[TopicMessage],
        deserializer: Serializer | None = None,
        return_type: type[TopicMessage] | None = None,
        serializer: Serializer | None = None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        self.name = name
        self.deserializer = deserializer
        self.container = container
        self.publish_fn = publish_fn
        self.exception_handlers = exception_handlers
        self.deserialization_error_handler = deserialization_error_handler
        self.dependent = self.container.solve(
            Dependent(func, scope="consumer"), scopes=Scopes
        )
        self.param_type = param_type
        self.return_type = return_type
        self.serializer = serializer
        self.sink_topics = sink_topics
        self.executor = AsyncExecutor()

    def prepare(self, state: ScopeState) -> None:
        self.container_state = state

    async def _deserialize(self, record: ConsumerRecord) -> Any:
        if not self.deserializer:
            return record.value
        try:
            deserialized = self.deserializer.deserialize(record.value)
            return deserialized
        except Exception as e:
            if not self.deserialization_error_handler:
                raise KaflowDeserializationException(
                    f"Failed to deserialize message from topic `{self.name}`",
                    record=record,
                ) from e
            await self.deserialization_error_handler(e, record)
            return None

    async def _execute_dependent(
        self, consumer_state: ScopeState, message: TopicMessage
    ) -> TopicMessage | None:
        try:
            return await self.dependent.execute_async(
                executor=self.executor,
                state=consumer_state,
                values={self.param_type: message},
            )
        except tuple(self.exception_handlers.keys()) as e:
            await self.exception_handlers[type(e)](e)
            return None

    def _validate_message(self, message: TopicMessage) -> None:
        if self.return_type and not isinstance(message, self.return_type):
            func_name = self.dependent.dependency.call.__name__  # type: ignore
            raise TypeError(
                f"Return type of `{func_name}` function is not of"
                f" `{self.return_type.__name__}` type"
            )

    def _publish_messages(self, message: TopicMessage) -> None:
        if self.sink_topics and self.serializer and message:
            message = _serialize(message, self.serializer)
            for topic in self.sink_topics:
                asyncio.create_task(self.publish_fn(topic, message))

    async def _process(self, message: TopicMessage) -> None:
        async with self.container.enter_scope(
            "consumer", state=self.container_state
        ) as consumer_state:
            message = await self._execute_dependent(
                consumer_state=consumer_state, message=message
            )
        if message:
            self._validate_message(message)
            self._publish_messages(message)

    async def consume(self, record: ConsumerRecord) -> None:
        message = await self._deserialize(record)
        if message is None:
            return
        if (
            hasattr(self.param_type, "__bases__")
            and BaseModel in self.param_type.__bases__
        ):
            message = self.param_type(**message)
        await self._process(message=message)
