from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Sequence

from di.dependent import Dependent
from di.executors import AsyncExecutor
from pydantic import BaseModel

from kaflow._utils.asyncio import task_group
from kaflow._utils.di import Scopes
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
        "return_model_type",
        "serializer_type",
        "sink_topics",
        "executor",
    )

    def __init__(
        self,
        dependent: SolvedDependent[Any],
        container: Container,
        return_model_type: type[BaseModel] | None = None,
        serializer_type: type[Serializer] | None = None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        self.dependent = dependent
        self.container = container
        self.return_model_type = return_model_type
        self.serializer_type = serializer_type
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
                values={BaseModel: model},
            )
        if (
            return_model
            and self.return_model_type
            and not isinstance(return_model, self.return_model_type)
        ):
            func_name = self.dependent.dependency.call.__name__  # type: ignore
            raise TypeError(
                f"Return type of `{func_name}` function is not of"
                f" `{self.return_model_type.__name__}` type"
            )

        if self.sink_topics and self.serializer_type and return_model:
            message = self.serializer_type.serialize(return_model)
            for topic in self.sink_topics:
                asyncio.create_task(callback_fn(topic, message))


class TopicProcessor:
    __slots__ = (
        "name",
        "param_type",
        "deserializer",
        "container",
        "container_state",
        "funcs",
    )

    def __init__(
        self,
        name: str,
        param_type: type[BaseModel],
        deserializer: type[Serializer],
        container: Container,
    ) -> None:
        self.name = name
        self.param_type = param_type
        self.deserializer = deserializer
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
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        self.funcs.append(
            TopicProcessingFunc(
                dependent=self.container.solve(
                    Dependent(func, scope="consumer"), scopes=Scopes
                ),
                container=self.container,
                return_model_type=return_type,
                serializer_type=serializer,
                sink_topics=sink_topics,
            )
        )

    async def distribute(
        self,
        record: ConsumerRecord,
        callback_fn: Callable[[str, bytes], Coroutine[Any, Any, None]],
    ) -> None:
        raw = self.deserializer.deserialize(record.value)
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
