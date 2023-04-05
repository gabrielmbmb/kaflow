from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from contextlib import asynccontextmanager
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Literal,
    Sequence,
    Union,
)

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from aiokafka.helpers import create_ssl_context
from di import Container, ScopeState
from di.dependent import Dependent
from di.executors import AsyncExecutor

from kaflow import _parameters
from kaflow._consumer import TopicConsumerFunc
from kaflow._utils.asyncio import asyncify
from kaflow._utils.inspect import is_not_coroutine_function
from kaflow.asyncapi._builder import build_asyncapi
from kaflow.dependencies import Scopes
from kaflow.message import Message

if TYPE_CHECKING:
    from kaflow.asyncapi.models import AsyncAPI
    from kaflow.serializers import Serializer
    from kaflow.typing import TopicValueKeyHeader

ConsumerFunc = Callable[..., Union[Message, Awaitable[Union[Message, None]], None]]
ProducerFunc = Callable[..., Union[Message, Awaitable[Message]]]
ExceptionHandlerFunc = Callable[[Exception], Awaitable]
DeserializationErrorHandlerFunc = Callable[[Exception, ConsumerRecord], Awaitable[None]]


SecurityProtocol = Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
SaslMechanism = Literal[
    "PLAIN", "GSSAPI", "OAUTHBEARER", "SCRAM-SHA-256", "SCRAM-SHA-512"
]
AutoOffsetReset = Literal["earliest", "latest", "none"]


class Kaflow:
    def __init__(
        self,
        client_id: str,
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
        asyncapi_version: str = "2.6.0",
        title: str = "Kaflow",
        version: str = "0.0.1",
        description: str | None = None,
        terms_of_service: str | None = None,
        contact: dict[str, str | Any] = None,
        license_info: dict[str, str | Any] = None,
    ) -> None:
        # AIOKafka
        self.client_id = client_id
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

        # AsyncAPI
        self.asyncapi_version = asyncapi_version
        self.title = title
        self.version = version
        self.description = description
        self.terms_of_service = terms_of_service
        self.contact = contact
        self.license_info = license_info
        self.asyncapi_schema: AsyncAPI | None = None

        self._container = Container()
        self._container_state = ScopeState()

        self._loop = asyncio.get_event_loop()
        self._consumer: AIOKafkaConsumer | None = None
        self._producer: AIOKafkaProducer | None = None

        self._consumers: dict[str, TopicConsumerFunc] = {}
        self._producers: dict[str, list[ProducerFunc]] = defaultdict(list)
        self._sink_topics: set[str] = set()

        self._exception_handlers: dict[
            type[Exception], Callable[..., Awaitable[None]]
        ] = {}
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
        for topic_processor in self._consumers.values():
            topic_processor.prepare(self._container_state)

    def _add_topic_consumer_func(
        self,
        topic: str,
        func: ConsumerFunc,
        value_param_type: type[TopicValueKeyHeader],
        value_deserializer: Serializer | None = None,
        key_param_type: type[TopicValueKeyHeader] | None = None,
        key_deserializer: Serializer | None = None,
        headers_type_deserializers: dict[
            str, tuple[type[TopicValueKeyHeader], Serializer | None]
        ]
        | None = None,
        sink_topics: Sequence[str] | None = None,
    ) -> None:
        topic_processor = TopicConsumerFunc(
            name=topic,
            container=self._container,
            publish_fn=self._publish,
            exception_handlers=self._exception_handlers,
            deserialization_error_handler=self._deserialization_error_handler,
            func=func,
            value_param_type=value_param_type,
            value_deserializer=value_deserializer,
            key_param_type=key_param_type,
            key_deserializer=key_deserializer,
            headers_type_deserializers=headers_type_deserializers,
            sink_topics=sink_topics,
        )
        self._consumers[topic] = topic_processor

    def _create_consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            *self._consumers.keys(),
            loop=self._loop,
            bootstrap_servers=self.brokers,
            client_id=self.client_id,
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
            client_id=self.client_id,
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
            (
                value_param_type,
                value_deserializer,
                key_param_type,
                key_deserializer,
                headers_type_deserializers,
            ) = _parameters.get_function_parameters_info(func)
            self._add_topic_consumer_func(
                topic=topic,
                func=func,
                value_param_type=value_param_type,
                value_deserializer=value_deserializer,
                key_param_type=key_param_type,
                key_deserializer=key_deserializer,
                headers_type_deserializers=headers_type_deserializers,
                sink_topics=sink_topics,
            )
            if sink_topics:
                self._sink_topics.update(sink_topics)
            if is_not_coroutine_function(func):
                func = asyncify(func)
            return func

        return register_consumer

    def produce(self, sink_topic: str) -> Callable[[ProducerFunc], ProducerFunc]:
        def register_producer(func: ProducerFunc) -> Callable[..., Any]:
            self._sink_topics.update([sink_topic])

            def _create_coro(topic: str, message: Any) -> Coroutine[Any, Any, None]:
                if not isinstance(message, Message):
                    raise ValueError()
                return self._publish(
                    topic=topic,
                    value=message.value,
                    key=message.key,
                    headers=message.headers,
                    partition=message.partition,
                    timestamp=message.timestamp,
                )

            if is_not_coroutine_function(func):

                @wraps(func)
                def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                    message = func(*args, **kwargs)
                    asyncio.run_coroutine_threadsafe(
                        coro=_create_coro(sink_topic, message),
                        loop=self._loop,
                    )
                    return message

                return sync_wrapper

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                message = await func(*args, **kwargs)  # type: ignore
                await _create_coro(sink_topic, message)
                return message

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

    def asyncapi(self) -> AsyncAPI:
        if not self.asyncapi_schema:
            self.asyncapi_schema = build_asyncapi(
                asyncapi_version=self.asyncapi_version,
                title=self.title,
                version=self.version,
                description=self.description,
                terms_of_service=self.terms_of_service,
                contact=self.contact,
                license_info=self.license_info,
                consumers=self._consumers,
                producers=self._producers,
            )
        return self.asyncapi_schema

    async def _publish(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: dict[str, bytes] | None = None,
        partition: int | None = None,
        timestamp: int | None = None,
    ) -> None:
        if not self._producer:
            raise RuntimeError(
                "The producer has not been started yet. You're probably seeing this"
                f" error because `{self.__class__.__name__}.start` method has not been"
                " called yet."
            )

        if isinstance(headers, dict):
            headers_ = [(k, v) for k, v in headers.items()]
        else:
            headers_ = None

        await self._producer.send_and_wait(
            topic=topic,
            value=value,
            key=key,
            partition=partition,
            timestamp_ms=timestamp,
            headers=headers_,
        )

    async def _consuming_loop(self) -> None:
        if not self._consumer:
            raise RuntimeError(
                "The consumer has not been started yet. You're probably seeing this"
                f" error because `{self.__class__.__name__}.start` method has not been"
                " called yet."
            )
        async for record in self._consumer:
            await self._consumers[record.topic].consume(record=record)

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
        return list(self._consumers.keys())

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
