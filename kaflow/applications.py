from __future__ import annotations

import asyncio
import functools
import inspect
from contextlib import asynccontextmanager
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Literal,
    Sequence,
)

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
from di import Container, ScopeState
from di.dependent import Dependent
from di.executors import AsyncExecutor
from pydantic import BaseModel

from kaflow._utils.asyncio import asyncify
from kaflow._utils.inspect import (
    annotated_param_with,
    has_return_annotation,
    is_not_coroutine_function,
    signature_contains_annotated_param_with,
)
from kaflow.dependencies import Scopes
from kaflow.serializers import MESSAGE_SERIALIZER_FLAG
from kaflow.topic import TopicProcessor

if TYPE_CHECKING:
    from kaflow.serializers import Serializer
    from kaflow.typing import ConsumerFunc, ProducerFunc


def annotated_serializer_info(
    param: Any,
) -> tuple[type[BaseModel], type[Serializer], dict[str, Any]]:
    """Get the type and serializer of a parameter annotated (`Annotated[...]`) with
    a Kaflow serializer. This function expect the parameter to be `Annotated` with a
    Kaflow serializer, the `kaflow.serializers.MESSAGE_SERIALIZER_FLAG`, and optionally
    with a `dict` containing extra information that could be used by the serializer.

    Args:
        param: the annotated parameter to get the type and serializer from.

    Returns:
        A tuple with the type and serializer of the annotated parameter, and a `dict`
        containing extra information that could be used by the serializer.
    """
    param_type = param.__args__[0]
    serializer = param.__metadata__[0]
    if len(param.__metadata__) == 3:
        extra_metadata = param.__metadata__[2]
        extra_annotations_keys = serializer.extra_annotations_keys()
        missing_keys = extra_annotations_keys - extra_metadata.keys()
        if missing_keys:
            raise ValueError(
                f"Missing keys in extra metadata for {serializer.__name__}: "
                f"{missing_keys}"
            )
        extra_dict = {k: extra_metadata[k] for k in extra_annotations_keys}
        return param_type, serializer, extra_dict
    return param_type, serializer, {}


SecurityProtocol = Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
SaslMechanism = Literal[
    "PLAIN", "GSSAPI", "OAUTHBEARER", "SCRAM-SHA-256", "SCRAM-SHA-512"
]
AutoOffsetReset = Literal["earliest", "latest", "none"]


class Kaflow:
    def __init__(
        self,
        name: str,
        brokers: str | list[str],
        group_id: str | None = None,
        security_protocol: SecurityProtocol = "PLAINTEXT",
        cafile: str | None = None,
        capath: str | None = None,
        cadata: bytes | None = None,
        certfile: str | None = None,
        keyfile: str | None = None,
        cert_password: str | None = None,
        sasl_mechanism: SaslMechanism | None = None,
        sasl_plain_username: str | None = None,
        sasl_plain_password: str | None = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
        lifespan: Callable[..., AsyncContextManager[None]] | None = None,
    ) -> None:
        self.name = name
        self.brokers = brokers
        self.group_id = group_id
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        self.auto_offset_reset = auto_offset_reset
        self.auto_commit = auto_commit
        self.auto_commit_interval_ms = auto_commit_interval_ms

        if security_protocol == "SSL" or security_protocol == "SASL_SSL":
            self.ssl_context = create_ssl_context(
                cafile=cafile,
                capath=capath,
                cadata=cadata,
                certfile=certfile,
                keyfile=keyfile,
                password=cert_password,
            )
        else:
            self.ssl_context = None

        self._container = Container()
        self._container_state = ScopeState()

        self._loop = asyncio.get_event_loop()
        self._consumer: AIOKafkaConsumer | None = None
        self._producer: AIOKafkaProducer | None = None

        self._topics_processors: dict[str, TopicProcessor] = {}
        self._topics_producers: dict[str, TopicProcessor] = {}

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
        deserializer_extra: dict[str, Any],
        return_type: type[BaseModel] | None,
        serializer: type[Serializer] | None,
        serializer_extra: dict[str, Any] | None,
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
                deserializer_extra=deserializer_extra,
                container=self._container,
            )
        topic_processor.add_func(
            func=func,
            return_type=return_type,
            serializer=serializer,
            serializer_extra=serializer_extra,
            sink_topics=sink_topics,
        )
        self._topics_processors[topic] = topic_processor

    def _create_consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            *self._topics_processors.keys(),
            loop=self._loop,
            bootstrap_servers=self.brokers,
            client_id=self.name,
            group_id=self.group_id,
            ssl_context=self.ssl_context,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            enable_auto_commit=self.auto_commit,
            auto_offset_reset=self.auto_offset_reset,
            auto_commit_interval_ms=self.auto_commit_interval_ms,
        )

    def _create_producer(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            loop=self._loop,
            bootstrap_servers=self.brokers,
            client_id=self.name,
            ssl_context=self.ssl_context,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
        )

    def consume(
        self, topic: str, sink_topics: Sequence[str] | None = None
    ) -> Callable[[ConsumerFunc], ConsumerFunc]:
        def register_consumer(func: ConsumerFunc) -> ConsumerFunc:
            signature = inspect.signature(func)
            message_serializer_param = signature_contains_annotated_param_with(
                MESSAGE_SERIALIZER_FLAG, signature
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
            serializer: type[Serializer] | None = None
            serializer_extra: dict[str, Any] | None = None
            if has_return_annotation(signature):
                if not annotated_param_with(
                    MESSAGE_SERIALIZER_FLAG, signature.return_annotation
                ):
                    raise ValueError(
                        f"`{signature.return_annotation}` cannot be used as a return"
                        f" type for '{func.__name__}' consumer function. Consumer"
                        " functions must return a message annotated with a serializer"
                        " like `kaflow.serializers.Json`: `async def"
                        f" {func.__name__}(message: Json[BaseModel]) ->"
                        " Json[BaseModel]: ...`"
                    )
                else:
                    (
                        return_type,
                        serializer,
                        serializer_extra,
                    ) = annotated_serializer_info(signature.return_annotation)
            param_type, deserializer, deserializer_extra = annotated_serializer_info(
                message_serializer_param.annotation
            )
            self._add_topic_processor(
                topic=topic,
                func=func,
                param_type=param_type,
                deserializer=deserializer,
                deserializer_extra=deserializer_extra,
                return_type=return_type,
                serializer=serializer,
                serializer_extra=serializer_extra,
                sink_topics=sink_topics,
            )
            if is_not_coroutine_function(func):
                func = asyncify(func)
            return func

        return register_consumer

    def produce(self, sink_topic: str) -> Callable[[ProducerFunc], ProducerFunc]:
        def register_producer(func: ProducerFunc) -> Callable[..., Any]:
            signature = inspect.signature(func)
            if not has_return_annotation(signature) or not annotated_param_with(
                MESSAGE_SERIALIZER_FLAG, signature.return_annotation
            ):
                raise ValueError(
                    f"`{signature.return_annotation}` cannot be used as a return type"
                    f" for '{func.__name__}' consumer function. Producer functions must"
                    " return a message annotated with a serializer like"
                    f" `kaflow.serializers.Json`: `async def {func.__name__}() ->"
                    " Json[BaseModel]: ...`"
                )
            (_, serializer, serializer_extra) = annotated_serializer_info(
                signature.return_annotation
            )

            serialize = functools.partial(
                serializer.serialize, **serializer_extra or {}
            )

            if is_not_coroutine_function(func):

                @wraps(func)
                def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                    r = func(*args, **kwargs)
                    message = serialize(r)
                    asyncio.run_coroutine_threadsafe(
                        coro=self._publish(topic=sink_topic, value=message),
                        loop=self._loop,
                    )
                    return r

                return sync_wrapper

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                r = await func(*args, **kwargs)  # type: ignore
                message = serialize(r)
                await self._publish(topic=sink_topic, value=message)
                return r

            return async_wrapper

        return register_producer

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
