from __future__ import annotations

import asyncio
import inspect
from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Coroutine,
    NamedTuple,
)

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from di import Container, ScopeState
from di.dependent import Dependent
from di.executors import AsyncExecutor
from pydantic import BaseModel

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord
    from di import SolvedDependent

    from kaflow.serializers import Serializer
    from kaflow.typing import ConsumerFunc


def _get_consume_func_info(
    func: ConsumerFunc,
) -> tuple[
    type[BaseModel], type[Serializer], type[BaseModel] | None, type[Serializer] | None
]:
    """Get the model and deserializer from the consume function.

    Args:
        func: The function consuming messages from a Kafka topic. It must have
            only one argument, which is the message to be consumed.

    Returns:
        A tuple containing the model and deserializer of the message to be consumed. If
        the function has a return type, it will also return the model and serializer of
        the return type.
    """
    annotated = list(func.__annotations__.values())[0]
    model = annotated.__args__[0]
    deserializer = annotated.__metadata__[0]
    return_annotation = func.__annotations__.get("return")
    return_model = None
    serializer = None
    if return_annotation:
        return_model = return_annotation.__args__[0]
        serializer = return_annotation.__metadata__[0]
    return model, deserializer, return_model, serializer


Scopes = ("app", "consumer")


class TopicProcessingFunc(NamedTuple):
    dependent: SolvedDependent[Any]
    container: Container
    container_state: ScopeState | None = None
    return_model_type: type[BaseModel] | None = None
    serializer_type: type[Serializer] | None = None
    sink_topics: list[str] | None = None
    executor: AsyncExecutor = AsyncExecutor()

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
        "model_type",
        "deserializer_type",
        "container",
        "container_state",
        "funcs",
    )

    def __init__(
        self,
        name: str,
        model_type: type[BaseModel],
        deserializer_type: type[Serializer],
        container: Container,
    ) -> None:
        self.name = name
        self.model_type = model_type
        self.deserializer_type = deserializer_type
        self.container = container
        self.funcs: list[TopicProcessingFunc] = []

    def __repr__(self) -> str:
        return (
            f"TopicProcessor(name={self.name}, model={self.model_type},"
            f" deserializer={self.deserializer_type})"
        )

    def prepare(self, state: ScopeState) -> None:
        self.container_state = state

    def add_func(
        self,
        func: ConsumerFunc,
        return_model_type: type[BaseModel] | None,
        serializer_type: type[Serializer] | None,
        sink_topics: list[str] | None = None,
    ) -> None:
        self.funcs.append(
            TopicProcessingFunc(
                dependent=self.container.solve(
                    Dependent(func, scope="consumer"), scopes=Scopes
                ),
                container=self.container,
                return_model_type=return_model_type,
                serializer_type=serializer_type,
                sink_topics=sink_topics,
            )
        )

    async def distribute(
        self,
        record: ConsumerRecord,
        callback_fn: Callable[[str, bytes], Coroutine[Any, Any, None]],
    ) -> None:
        raw = self.deserializer_type.deserialize(record.value)
        model = self.model_type(**raw)
        for func in self.funcs:
            asyncio.create_task(func(model, self.container_state, callback_fn))


class Kaflow:
    def __init__(
        self,
        name: str,
        brokers: str | list[str],
        auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
        lifespan: Callable[..., AsyncContextManager[None]] | None = None,
    ) -> None:
        self.name = name
        self.brokers = brokers
        self.auto_commit = auto_commit
        self.auto_commit_interval_ms = auto_commit_interval_ms

        self.container = Container()
        self._container_state = ScopeState()

        self.loop = asyncio.get_event_loop()
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None

        self.topics_processors: dict[str, TopicProcessor] = {}

        @asynccontextmanager
        async def lifespan_ctx() -> AsyncIterator[None]:
            executor = AsyncExecutor()
            dep: Dependent[Any]

            async with self._container_state.enter_scope(
                scope="app"
            ) as self._container_state:
                if lifespan:
                    dep = Dependent(
                        _wrap_lifespan_as_async_generator(lifespan), scope="app"
                    )
                else:
                    dep = Dependent(lambda: None, scope="app")
                solved = self.container.solve(dep, scopes=Scopes)
                try:
                    await solved.execute_async(
                        executor=executor, state=self._container_state
                    )
                    self._prepare()
                    yield
                finally:
                    self._container_state = ScopeState()

        self.lifespan = lifespan_ctx

    def _prepare(self) -> None:
        for topic_processor in self.topics_processors.values():
            topic_processor.prepare(self._container_state)

    def _add_topic_processor(
        self,
        topic: str,
        func: ConsumerFunc,
        model_type: type[BaseModel],
        deserializer_type: type[Serializer],
        return_model_type: type[BaseModel] | None,
        serializer_type: type[Serializer] | None,
        sink_topics: list[str] | None = None,
    ) -> None:
        topic_processor = self.topics_processors.get(topic)
        if topic_processor:
            if (
                topic_processor.model_type != model_type
                or topic_processor.deserializer_type != deserializer_type
            ):
                self.loop.run_until_complete(self._stop())
                raise TypeError(
                    f"Topic '{topic}' is already registered with a different model"
                    f" and/or deserializer. TopicProcessor: {topic_processor}."
                )
        else:
            topic_processor = TopicProcessor(
                name=topic,
                model_type=model_type,
                deserializer_type=deserializer_type,
                container=self.container,
            )
        topic_processor.add_func(
            func=func,
            return_model_type=return_model_type,
            serializer_type=serializer_type,
            sink_topics=sink_topics,
        )
        self.topics_processors[topic] = topic_processor

    def _create_consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            *self.topics_processors.keys(),
            loop=self.loop,
            bootstrap_servers=self.brokers,
            enable_auto_commit=self.auto_commit,
            auto_commit_interval_ms=self.auto_commit_interval_ms,
        )

    def _create_producer(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=self.brokers,
        )

    def consume(
        self, topic: str, sink_topics: list[str] | None = None
    ) -> Callable[[ConsumerFunc], ConsumerFunc]:
        def register_consumer(func: ConsumerFunc) -> ConsumerFunc:
            if not asyncio.iscoroutinefunction(func):
                raise ValueError(
                    "Consumer functions must be coroutines (`async def ...`). You're"
                    " probably seeing this error because one of the function that you"
                    f" have decorated using `@{self.__class__.__name__}.consume` method"
                    " is a regular function."
                )
            # TODO: check there is at least one argument Json or Avro
            # if func.__code__.co_argcount != 1:
            #     raise ValueError(
            #         "Consumer functions must have only one argument. You're probably"
            #         " seeing this error because one of the function that you have"
            #         f" decorated using `@{self.__class__.__name__}.consume` method"
            #         " takes more or less than one argument."
            #     )
            (
                model_type,
                deserializer_type,
                return_model_type,
                serializer_type,
            ) = _get_consume_func_info(func)
            self._add_topic_processor(
                topic=topic,
                func=func,
                model_type=model_type,
                deserializer_type=deserializer_type,
                return_model_type=return_model_type,
                serializer_type=serializer_type,
                sink_topics=sink_topics,
            )
            return func

        return register_consumer

    async def _publish(self, topic: str, value: bytes) -> None:
        if not self.producer:
            raise RuntimeError(
                "The producer has not been started yet. You're probably seeing this"
                f" error because `{self.__class__.__name__}.run` method has not been"
                " called yet."
            )
        await self.producer.send_and_wait(topic=topic, value=value)

    async def _consuming_loop(self) -> None:
        if not self.consumer:
            raise RuntimeError(
                "The consumer has not been started yet. You're probably seeing this"
                f" error because `{self.__class__.__name__}.run` method has not been"
                " called yet."
            )
        async for record in self.consumer:
            topic = record.topic
            await self.topics_processors[topic].distribute(record, self._publish)

    async def _start(self) -> None:
        self.consumer = self._create_consumer()
        self.producer = self._create_producer()

        async with self.lifespan():
            await self.consumer.start()
            await self.producer.start()
            await self._consuming_loop()

    async def _stop(self) -> None:
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

    def run(self) -> None:
        try:
            self.loop.run_until_complete(self._start())
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            pass
        finally:
            self.loop.run_until_complete(self._stop())
            self.loop.close()

    @property
    def topics(self) -> list[str]:
        return list(self.topics_processors.keys())


# Taken from adriandg/xpresso
# https://github.com/adriangb/xpresso/blob/0a69b5131440cd114baeab7243db7bd3255e66ed/xpresso/applications.py#L392
# Thanks :)
def _wrap_lifespan_as_async_generator(
    lifespan: Callable[..., AsyncContextManager[None]]
) -> Callable[..., AsyncIterator[None]]:
    # wrap true context managers in an async generator
    # so that the dependency injection system recognizes it
    async def gen(*args: Any, **kwargs: Any) -> AsyncIterator[None]:
        async with lifespan(*args, **kwargs):
            yield

    # this is so that the dependency injection system
    # still picks up parameters from the function signature
    sig = inspect.signature(gen)
    sig = sig.replace(parameters=list(inspect.signature(lifespan).parameters.values()))
    setattr(gen, "__signature__", sig)  # noqa: B010

    return gen
