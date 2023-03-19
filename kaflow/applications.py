from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord
    from pydantic import BaseModel

    from kaflow.serialize import Serializer
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


class TopicProcessingFunc:
    def __init__(
        self,
        func: ConsumerFunc,
        return_model_type: type[BaseModel] | None,
        serializer_type: type[Serializer] | None,
        sink_topics: list[str] | None = None,
    ) -> None:
        self.func = func
        self.return_model_type = return_model_type
        self.serializer_type = serializer_type
        self.sink_topics = sink_topics

    async def consume(
        self, model: BaseModel, callback_fn: Callable[[str, bytes], Awaitable[None]]
    ) -> None:
        return_model = await self.func(model)
        if (
            return_model
            and self.return_model_type
            and not isinstance(return_model, self.return_model_type)
        ):
            raise TypeError(
                f"Return type of {self.func.__name__} is not {self.return_model_type}"
            )

        if self.sink_topics and self.serializer_type and return_model:
            message = self.serializer_type.serialize(return_model)
            for topic in self.sink_topics:
                asyncio.create_task(callback_fn(topic, message))


class TopicProcessor:
    def __init__(
        self,
        name: str,
        model_type: type[BaseModel],
        deserializer_type: type[Serializer],
    ) -> None:
        self.name = name
        self.model_type = model_type
        self.deserializer_type = deserializer_type
        self.funcs: list[TopicProcessingFunc] = []

    def __repr__(self) -> str:
        return (
            f"TopicProcessor(name={self.name}, model={self.model_type},"
            f" deserializer={self.deserializer_type})"
        )

    def add_func(
        self,
        func: ConsumerFunc,
        return_model_type: type[BaseModel] | None,
        serializer_type: type[Serializer] | None,
        sink_topics: list[str] | None = None,
    ) -> None:
        self.funcs.append(
            TopicProcessingFunc(
                func=func,
                return_model_type=return_model_type,
                serializer_type=serializer_type,
                sink_topics=sink_topics,
            )
        )

    async def distribute(
        self,
        record: ConsumerRecord,
        callback_fn: Callable[[str, bytes], Awaitable[None]],
    ) -> None:
        raw = self.deserializer_type.deserialize(record.value)
        model = self.model_type(**raw)
        for func in self.funcs:
            asyncio.create_task(func.consume(model, callback_fn))


class Kaflow:
    def __init__(
        self,
        name: str,
        brokers: str | list[str],
        auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
    ) -> None:
        self.name = name
        self.brokers = brokers
        self.loop = asyncio.get_event_loop()

        self.auto_commit = auto_commit
        self.auto_commit_interval_ms = auto_commit_interval_ms
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=self.brokers,
            enable_auto_commit=self.auto_commit,
            auto_commit_interval_ms=self.auto_commit_interval_ms,
            loop=self.loop,
        )

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.brokers,
            loop=self.loop,
        )

        self.topics_processors: dict[str, TopicProcessor] = {}

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
                raise TypeError(
                    f"Topic '{topic}' is already registered with a different model"
                    f" and/or deserializer. TopicProcessor: {topic_processor}."
                )
        else:
            topic_processor = TopicProcessor(
                name=topic, model_type=model_type, deserializer_type=deserializer_type
            )
        topic_processor.add_func(
            func=func,
            return_model_type=return_model_type,
            serializer_type=serializer_type,
            sink_topics=sink_topics,
        )
        self.topics_processors[topic] = topic_processor

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
            if func.__code__.co_argcount != 1:
                raise ValueError(
                    "Consumer functions must have only one argument. You're probably"
                    " seeing this error because one of the function that you have"
                    f" decorated using `@{self.__class__.__name__}.subscribe` method"
                    " takes more or less than one argument."
                )
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
        await self.producer.send_and_wait(topic=topic, value=value)

    async def _consuming_loop(self) -> None:
        async for record in self.consumer:
            topic = record.topic
            await self.topics_processors[topic].distribute(record, self._publish)

    async def _start(self) -> None:
        self.consumer.subscribe(self.topics)
        await self.consumer.start()
        await self.producer.start()
        await self._consuming_loop()

    async def _stop(self) -> None:
        await self.consumer.stop()
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
