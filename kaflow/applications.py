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
    Sequence,
)

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from di import Container, ScopeState
from di.dependent import Dependent
from di.executors import AsyncExecutor
from pydantic import BaseModel

from kaflow._utils.di import Scopes
from kaflow.topic import TopicProcessor

if TYPE_CHECKING:
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

        self._container = Container()
        self._container_state = ScopeState()

        self._loop = asyncio.get_event_loop()
        self._consumer: AIOKafkaConsumer | None = None
        self._producer: AIOKafkaProducer | None = None

        self._topics_processors: dict[str, TopicProcessor] = {}

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
                solved = self._container.solve(dep, scopes=Scopes)
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
        for topic_processor in self._topics_processors.values():
            topic_processor.prepare(self._container_state)

    def _add_topic_processor(
        self,
        topic: str,
        func: ConsumerFunc,
        model_type: type[BaseModel],
        deserializer_type: type[Serializer],
        return_model_type: type[BaseModel] | None,
        serializer_type: type[Serializer] | None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        topic_processor = self._topics_processors.get(topic)
        if topic_processor:
            if (
                topic_processor.model_type != model_type
                or topic_processor.deserializer_type != deserializer_type
            ):
                self._loop.run_until_complete(self._stop())
                raise TypeError(
                    f"Topic '{topic}' is already registered with a different model"
                    f" and/or deserializer. TopicProcessor: {topic_processor}."
                )
        else:
            topic_processor = TopicProcessor(
                name=topic,
                model_type=model_type,
                deserializer_type=deserializer_type,
                container=self._container,
            )
        topic_processor.add_func(
            func=func,
            return_model_type=return_model_type,
            serializer_type=serializer_type,
            sink_topics=sink_topics,
        )
        self._topics_processors[topic] = topic_processor

    def _create_consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            *self._topics_processors.keys(),
            loop=self._loop,
            bootstrap_servers=self.brokers,
            enable_auto_commit=self.auto_commit,
            auto_commit_interval_ms=self.auto_commit_interval_ms,
        )

    def _create_producer(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            loop=self._loop,
            bootstrap_servers=self.brokers,
        )

    def consume(
        self, topic: str, sink_topics: Sequence[str] | None = None
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
        if not self._producer:
            raise RuntimeError(
                "The producer has not been started yet. You're probably seeing this"
                f" error because `{self.__class__.__name__}.run` method has not been"
                " called yet."
            )
        await self._producer.send_and_wait(topic=topic, value=value)

    async def _consuming_loop(self) -> None:
        if not self._consumer:
            raise RuntimeError(
                "The consumer has not been started yet. You're probably seeing this"
                f" error because `{self.__class__.__name__}.run` method has not been"
                " called yet."
            )
        async for record in self._consumer:
            topic = record.topic
            await self._topics_processors[topic].distribute(record, self._publish)

    async def _start(self) -> None:
        self._consumer = self._create_consumer()
        self._producer = self._create_producer()

        async with self.lifespan():
            await self._consumer.start()
            await self._producer.start()
            await self._consuming_loop()

    async def _stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()

    def run(self) -> None:
        try:
            self._loop.run_until_complete(self._start())
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            pass
        finally:
            self._loop.run_until_complete(self._stop())
            self._loop.close()

    @property
    def topics(self) -> list[str]:
        return list(self._topics_processors.keys())


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
