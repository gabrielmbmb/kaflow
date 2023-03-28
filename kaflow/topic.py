from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Sequence

from di.dependent import Dependent
from di.executors import AsyncExecutor
from pydantic import BaseModel

from kaflow._utils.asyncio import task_group
from kaflow.dependencies import Scopes
from kaflow.exceptions import KaflowDistributeException

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord
    from di import Container, ScopeState, SolvedDependent

    from kaflow.serializers import Serializer
    from kaflow.typing import ConsumerFunc


class TopicProcessingFunc:
    __slots__ = (
        "dependent",
        "container",
        "param_type",
        "return_type",
        "serializer",
        "serializer_extra",
        "sink_topics",
        "executor",
    )

    def __init__(
        self,
        dependent: SolvedDependent[Any],
        container: Container,
        param_type: type[BaseModel],
        return_type: type[BaseModel] | None = None,
        serializer: type[Serializer] | None = None,
        serializer_extra: dict[str, Any] | None = None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        self.dependent = dependent
        self.container = container
        self.param_type = param_type
        self.return_type = return_type
        self.serializer = serializer
        self.serializer_extra = serializer_extra or {}
        self.sink_topics = sink_topics
        self.executor = AsyncExecutor()

    async def __call__(
        self,
        model: BaseModel,
        state: ScopeState,
        callback_fn: Callable[[str, bytes], Coroutine[Any, Any, None]],
    ) -> None:
        async with self.container.enter_scope(
            "consumer", state=state
        ) as consumer_state:
            return_model = await self.dependent.execute_async(
                executor=self.executor,
                state=consumer_state,
                values={self.param_type: model},
            )
        if (
            return_model
            and self.return_type
            and not isinstance(return_model, self.return_type)
        ):
            func_name = self.dependent.dependency.call.__name__  # type: ignore
            raise TypeError(
                f"Return type of `{func_name}` function is not of"
                f" `{self.return_type.__name__}` type"
            )

        if self.sink_topics and self.serializer and return_model:
            message = self.serializer.serialize(return_model, **self.serializer_extra)
            for topic in self.sink_topics:
                asyncio.create_task(callback_fn(topic, message))


class TopicProcessor:
    __slots__ = (
        "name",
        "param_type",
        "deserializer",
        "deserializer_extra",
        "container",
        "container_state",
        "funcs",
    )

    def __init__(
        self,
        name: str,
        param_type: type[BaseModel],
        deserializer: type[Serializer],
        deserializer_extra: dict[str, Any],
        container: Container,
    ) -> None:
        self.name = name
        self.param_type = param_type
        self.deserializer = deserializer
        self.deserializer_extra = deserializer_extra
        self.container = container
        self.funcs: list[Callable[..., Coroutine[Any, Any, None]]] = []

    def __repr__(self) -> str:
        return (
            f"TopicProcessor(name={self.name}, model={self.param_type},"
            f" deserializer={self.deserializer})"
        )

    def prepare(self, state: ScopeState) -> None:
        self.container_state = state

    def add_func(
        self,
        func: ConsumerFunc,
        return_type: type[BaseModel] | None,
        serializer: type[Serializer] | None,
        serializer_extra: dict[str, Any] | None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        self.funcs.append(
            TopicProcessingFunc(
                dependent=self.container.solve(
                    Dependent(func, scope="consumer"), scopes=Scopes
                ),
                container=self.container,
                param_type=self.param_type,
                return_type=return_type,
                serializer=serializer,
                serializer_extra=serializer_extra,
                sink_topics=sink_topics,
            )
        )

    async def distribute(
        self,
        record: ConsumerRecord,
        callback_fn: Callable[[str, bytes], Coroutine[Any, Any, None]],
    ) -> None:
        raw = self.deserializer.deserialize(record.value, **self.deserializer_extra)
        model = self.param_type(**raw)
        try:
            await task_group(self.funcs, model, self.container_state, callback_fn)
        except Exception as e:
            raise KaflowDistributeException(
                (
                    "An error occurred while distributing message to functions"
                    f" consuming topic '{self.name}'."
                ),
                record=record,
            ) from e
