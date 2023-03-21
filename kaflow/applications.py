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
from kaflow._utils.inspect import (
    annotated_param_with,
    has_return_annotation,
    signature_contains_annotated_param_with,
)
from kaflow.serializers import MessageSerializerFlag
from kaflow.topic import TopicProcessor

if TYPE_CHECKING:
    from kaflow.serializers import Serializer
    from kaflow.typing import ConsumerFunc


def annotated_serializer_info(param: Any) -> tuple[type[BaseModel], type[Serializer]]:
    """Get the type and serializer of a parameter annotated (`Annotated[...]`) with
    a Kaflow serializer.

    Args:
        param: the annotated parameter to get the type and serializer from.

    Returns:
        A tuple with the type and serializer of the annotated parameter.
    """
    param_type = param.__args__[0]
    deserializer = param.__metadata__[0]
    return param_type, deserializer


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
        param_type: type[BaseModel],
        deserializer: type[Serializer],
        return_type: type[BaseModel] | None,
        serializer: type[Serializer] | None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        topic_processor = self._topics_processors.get(topic)
        if topic_processor:
            if (
                topic_processor.param_type != param_type
                or topic_processor.deserializer != deserializer
            ):
                self._loop.run_until_complete(self._stop())
                raise TypeError(
                    f"Topic '{topic}' is already registered with a different model"
                    f" and/or deserializer. TopicProcessor: {topic_processor}."
                )
        else:
            topic_processor = TopicProcessor(
                name=topic,
                param_type=param_type,
                deserializer=deserializer,
                container=self._container,
            )
        topic_processor.add_func(
            func=func,
            return_type=return_type,
            serializer=serializer,
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
                    f"'{func.__name__}' is not a coroutine. Consumer functions must be"
                    f" coroutines (`async def {func.__name__}`)."
                )
            signature = inspect.signature(func)
            message_serializer_param = signature_contains_annotated_param_with(
                MessageSerializerFlag, signature
            )
            if not message_serializer_param:
                raise ValueError(
                    f"'{func.__name__}' does not have a message parameter annotated"
                    " with a serializer like `kaflow.serializers.Json`: `async def"
                    f" {func.__name__}(message: Json[BaseModel]) -> None: ...`"
                    " Consumers functions must have a parameter annotated with a"
                    " serializer like `kaflow.serializers.Json` to receive the"
                    " messages from the topic."
                )
            return_type: Any | None = None
            serializer: Serializer | None = None
            if has_return_annotation(signature):
                if not annotated_param_with(
                    MessageSerializerFlag, signature.return_annotation
                ):
                    raise ValueError(
                        f"`{signature.return_annotation}` cannot be used as a return"
                        f" type for '{func.__name__}' consumer function. Consumers"
                        " functions must return a message annotated with a serializer"
                        " like `kaflow.serializers.Json`: `async def"
                        " process_topic(message: Json[BaseModel]) -> Json[BaseModel]:"
                        " ...`"
                    )
                else:
                    return_type, serializer = annotated_serializer_info(
                        signature.return_annotation
                    )
            param_type, deserializer = annotated_serializer_info(
                message_serializer_param.annotation
            )
            self._add_topic_processor(
                topic=topic,
                func=func,
                param_type=param_type,
                deserializer=deserializer,
                return_type=return_type,
                serializer=serializer,
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
