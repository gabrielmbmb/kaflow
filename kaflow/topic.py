from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Coroutine, Sequence

from di.dependent import Dependent
from di.executors import AsyncExecutor
from pydantic import BaseModel

from kaflow.dependencies import Scopes
from kaflow.exceptions import KaflowDeserializationException
from kaflow.serializers import _serialize

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord
    from di import Container, ScopeState, SolvedDependent

    from kaflow.serializers import Serializer
    from kaflow.typing import (
        ConsumerFunc,
        DeserializationErrorHandlerFunc,
        ExceptionHandlerFunc,
        TopicMessage,
    )


class TopicProcessingFunc:
    __slots__ = (
        "dependent",
        "container",
        "param_type",
        "return_type",
        "serializer",
        "sink_topics",
        "executor",
    )

    def __init__(
        self,
        dependent: SolvedDependent[TopicMessage | None],
        container: Container,
        param_type: type[TopicMessage],
        return_type: type[TopicMessage] | None = None,
        serializer: Serializer | None = None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        self.dependent = dependent
        self.container = container
        self.param_type = param_type
        self.return_type = return_type
        self.serializer = serializer
        self.sink_topics = sink_topics
        self.executor = AsyncExecutor()

    async def _execute_dependent(
        self,
        consumer_state: ScopeState,
        message: TopicMessage,
        exception_handlers: dict[
            type[Exception], Callable[[Exception], Awaitable[Any]]
        ],
    ) -> TopicMessage | None:
        try:
            return await self.dependent.execute_async(
                executor=self.executor,
                state=consumer_state,
                values={self.param_type: message},
            )
        except tuple(exception_handlers.keys()) as e:
            await exception_handlers[type(e)](e)
            return None

    def _validate_message(self, message: TopicMessage) -> None:
        if self.return_type and not isinstance(message, self.return_type):
            func_name = self.dependent.dependency.call.__name__  # type: ignore
            raise TypeError(
                f"Return type of `{func_name}` function is not of"
                f" `{self.return_type.__name__}` type"
            )

    def _publish_messages(
        self,
        message: TopicMessage,
        publish_fn: Callable[[str, bytes], Coroutine[Any, Any, None]],
    ) -> None:
        if self.sink_topics and self.serializer and message:
            message = _serialize(message, self.serializer)
            for topic in self.sink_topics:
                asyncio.create_task(publish_fn(topic, message))

    async def __call__(
        self,
        message: TopicMessage,
        state: ScopeState,
        publish_fn: Callable[[str, bytes], Coroutine[Any, Any, None]],
        exception_handlers: dict[
            type[Exception], Callable[[Exception], Awaitable[Any]]
        ],
    ) -> None:
        async with self.container.enter_scope(
            "consumer", state=state
        ) as consumer_state:
            message = await self._execute_dependent(
                consumer_state=consumer_state,
                message=message,
                exception_handlers=exception_handlers,
            )
        if message:
            self._validate_message(message)
            self._publish_messages(message, publish_fn)


class PythonObject:
    """Dummy class to represent a Python object."""

    pass


class TopicProcessor:
    __slots__ = (
        "name",
        "deserializer",
        "container",
        "publish_fn",
        "exception_handlers",
        "deserialization_error_handler",
        "funcs",
        "container_state",
    )

    def __init__(
        self,
        name: str,
        container: Container,
        publish_fn: Callable[[str, bytes], Coroutine[Any, Any, None]],
        exception_handlers: dict[type[Exception], ExceptionHandlerFunc],
        deserialization_error_handler: DeserializationErrorHandlerFunc | None = None,
        deserializer: Serializer | None = None,
    ) -> None:
        self.name = name
        self.deserializer = deserializer
        self.container = container
        self.publish_fn = publish_fn
        self.exception_handlers = exception_handlers
        self.deserialization_error_handler = deserialization_error_handler
        self.funcs: dict[
            type[TopicMessage], list[Callable[..., Coroutine[Any, Any, None]]]
        ] = defaultdict(list)

    def prepare(self, state: ScopeState) -> None:
        self.container_state = state

    def add_func(
        self,
        func: ConsumerFunc,
        param_type: type[TopicMessage],
        return_type: type[TopicMessage] | None = None,
        serializer: Serializer | None = None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        key: type[TopicMessage] = PythonObject
        if param_type is bytes or (
            hasattr(param_type, "__bases__") and BaseModel in param_type.__bases__
        ):
            key = param_type
        self.funcs[key].append(
            TopicProcessingFunc(
                dependent=self.container.solve(
                    Dependent(func, scope="consumer"), scopes=Scopes
                ),
                container=self.container,
                param_type=param_type,
                return_type=return_type,
                serializer=serializer,
                sink_topics=sink_topics,
            )
        )

    def _create_partial_func(
        self,
        message: TopicMessage,
        func: Callable[..., Coroutine[TopicMessage | None, Any, Any]],
    ) -> asyncio.Task[TopicMessage | None]:
        return asyncio.create_task(
            func(
                message=message,
                state=self.container_state,
                publish_fn=self.publish_fn,
                exception_handlers=self.exception_handlers,
            )
        )

    async def distribute(
        self,
        record: ConsumerRecord,
    ) -> None:
        deserialized = None
        if self.deserializer:
            try:
                deserialized = self.deserializer.deserialize(record.value)
            except Exception as e:
                if not self.deserialization_error_handler:
                    raise KaflowDeserializationException(
                        f"Failed to deserialize message from topic `{self.name}`",
                        record=record,
                    ) from e
                await self.deserialization_error_handler(e, record)

        tasks = []
        for expected_type in self.funcs:
            if expected_type is bytes:
                tasks.extend(
                    [
                        self._create_partial_func(record.value, func)
                        for func in self.funcs[expected_type]
                    ]
                )
                continue

            if deserialized:
                if expected_type is PythonObject:
                    tasks.extend(
                        [
                            self._create_partial_func(deserialized, func)
                            for func in self.funcs[expected_type]
                        ]
                    )
                    continue

                if BaseModel in expected_type.__bases__:
                    model = expected_type(**deserialized)
                    tasks.extend(
                        [
                            self._create_partial_func(model, func)
                            for func in self.funcs[expected_type]
                        ]
                    )
                    continue

        await asyncio.gather(*tasks)
