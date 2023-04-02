from __future__ import annotations

import asyncio
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
)
from kaflow.dependencies import Scopes
from kaflow.serializers import MESSAGE_SERIALIZER_FLAG
from kaflow.topic import TopicProcessor
from kaflow.typing import TopicMessage

if TYPE_CHECKING:
    from kaflow.serializers import Serializer
    from kaflow.typing import (
        ConsumerFunc,
        DeserializationErrorHandlerFunc,
        ExceptionHandlerFunc,
        ProducerFunc,
    )


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


def get_message_param_info(
    param: Any,
) -> tuple[type[TopicMessage], type[Serializer] | None, dict[str, Any]]:
    if param is bytes:
        return bytes, None, {}

    return annotated_serializer_info(param)


def bytes_or_serializer_param(signature: inspect.Signature) -> inspect.Parameter | None:
    """Get the parameter annotated with `kaflow.serializers.MESSAGE_SERIALIZER_FLAG`
    or of type `bytes` from a function signature.

    Args:
        signature: The function signature to get the parameter from.

    Returns:
        The parameter annotated with `kaflow.serializers.MESSAGE_SERIALIZER_FLAG` or of
        type `bytes` from the function signature, or `None` if no such parameter is
        found.
    """
    for param in signature.parameters.values():
        if param.annotation is bytes:
            return param
        if annotated_param_with(MESSAGE_SERIALIZER_FLAG, param.annotation):
            return param
    return None


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
        self._sink_topics: set[str] = set()

        self._exception_handlers: dict[type[Exception], ExceptionHandlerFunc] = {}
        self._deserialization_error_handler: DeserializationErrorHandlerFunc | None = (
            None
        )

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
        param_type: type[TopicMessage],
        deserializer: Serializer | None = None,
        return_type: type[TopicMessage] | None = None,
        serializer: Serializer | None = None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        topic_processor = self._topics_processors.get(topic)
        if topic_processor and deserializer:
            if type(topic_processor.deserializer) != type(deserializer):
                self._loop.run_until_complete(self.stop())
                raise TypeError(
                    f"Topic {topic} has already been registered with a different "
                    f"deserializer: {topic_processor.deserializer}."
                )
        if not topic_processor:
            topic_processor = TopicProcessor(
                name=topic,
                container=self._container,
                publish_fn=self._publish,
                exception_handlers=self._exception_handlers,
                deserialization_error_handler=self._deserialization_error_handler,
                deserializer=deserializer,
            )
        topic_processor.add_func(
            func=func,
            param_type=param_type,
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
        self,
        topic: str,
        sink_topics: Sequence[str] | None = None,
    ) -> Callable[[ConsumerFunc], ConsumerFunc]:
        def register_consumer(func: ConsumerFunc) -> ConsumerFunc:
            signature = inspect.signature(func)
            message_param = bytes_or_serializer_param(signature)
            if not message_param:
                raise ValueError(
                    f"'{func.__name__}' function does not have a parameter with a type"
                    " like `bytes` or a `pydantic.BaseModel` annotated with a"
                    " deserializer like `kaflow.serializers.Json` to receive the"
                    f" message from the topic: `async def  {func.__name__}(message:"
                    " Json[BaseModel]): ...`."
                )
            return_type: Any | None = None
            serializer_type: type[Serializer] | None = None
            serializer_extra: dict[str, Any] = {}
            if has_return_annotation(signature):
                if (
                    not annotated_param_with(
                        MESSAGE_SERIALIZER_FLAG, signature.return_annotation
                    )
                    and signature.return_annotation is not bytes
                ):
                    raise ValueError(
                        f"`{signature.return_annotation.__name__}` cannot be used as a"
                        f" return type for '{func.__name__}' consumer function."
                        " Consumer functions must return bytes or a"
                        " `pydantic.BaseModel` annotated with a serializer like"
                        " `kaflow.serializers.Json` so it can be published to the"
                        f" `sink_topics`: `async def {func.__name__}(message:"
                        " Json[BaseModel]) -> Json[BaseModel]: ...`."
                    )
                else:
                    (return_type, serializer_type, serializer_extra) = (
                        get_message_param_info(signature.return_annotation)
                    )
            param_type, deserializer_type, deserializer_extra = get_message_param_info(
                message_param.annotation
            )
            deserializer = None
            if deserializer_type:
                deserializer = deserializer_type(**deserializer_extra)
            serializer = None
            if serializer_type:
                serializer = serializer_type(**serializer_extra)
            self._add_topic_processor(
                topic=topic,
                func=func,
                param_type=param_type,
                deserializer=deserializer,
                return_type=return_type,
                serializer=serializer,
                sink_topics=sink_topics,
            )
            self._sink_topics.update(sink_topics or [])
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
            (_, serializer_type, serializer_extra) = annotated_serializer_info(
                signature.return_annotation
            )
            serializer = serializer_type(**serializer_extra)
            self._sink_topics.update([sink_topic])

            if is_not_coroutine_function(func):

                @wraps(func)
                def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                    r = func(*args, **kwargs)
                    message = serializer.serialize(r)
                    asyncio.run_coroutine_threadsafe(
                        coro=self._publish(topic=sink_topic, value=message),
                        loop=self._loop,
                    )
                    return r

                return sync_wrapper

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                r = await func(*args, **kwargs)  # type: ignore
                message = serializer.serialize(r)
                await self._publish(topic=sink_topic, value=message)
                return r

            return async_wrapper

        return register_producer

    def exception_handler(
        self, exception: type[Exception]
    ) -> Callable[[ExceptionHandlerFunc], ExceptionHandlerFunc]:
        def register_exception_handler(
            func: ExceptionHandlerFunc,
        ) -> ExceptionHandlerFunc:
            if is_not_coroutine_function(func):
                func = asyncify(func)
            self._exception_handlers[exception] = func
            return func

        return register_exception_handler

    def deserialization_error_handler(
        self,
    ) -> Callable[[DeserializationErrorHandlerFunc], DeserializationErrorHandlerFunc]:
        def register_deserialization_error_handler(
            func: DeserializationErrorHandlerFunc,
        ) -> DeserializationErrorHandlerFunc:
            self._deserialization_error_handler = func
            return func

        return register_deserialization_error_handler

    async def _publish(self, topic: str, value: bytes) -> None:
        if not self._producer:
            raise RuntimeError(
                "The producer has not been started yet. You're probably seeing this"
                f" error because `{self.__class__.__name__}.start` method has not been"
                " called yet."
            )
        await self._producer.send_and_wait(topic=topic, value=value)

    async def _consuming_loop(self) -> None:
        if not self._consumer:
            raise RuntimeError(
                "The consumer has not been started yet. You're probably seeing this"
                f" error because `{self.__class__.__name__}.start` method has not been"
                " called yet."
            )
        async for record in self._consumer:
            await self._topics_processors[record.topic].distribute(record=record)

    async def start(self) -> None:
        self._consumer = self._create_consumer()
        self._producer = self._create_producer()

        async with self.lifespan():
            await self._consumer.start()
            await self._producer.start()
            await self._consuming_loop()

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()

    def run(self) -> None:
        try:
            self._loop.run_until_complete(self.start())
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            pass
        finally:
            self._loop.run_until_complete(self.stop())
            self._loop.close()

    @property
    def consumed_topics(self) -> list[str]:
        return list(self._topics_processors.keys())

    @property
    def sink_topics(self) -> list[str]:
        return list(self._sink_topics)


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
