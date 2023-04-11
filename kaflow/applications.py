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
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
from di import Container, ScopeState
from di.dependent import Dependent
from di.executors import AsyncExecutor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner.default import DefaultPartitioner

from kaflow import parameters
from kaflow._consumer import TopicConsumerFunc
from kaflow._utils.asyncio import asyncify
from kaflow._utils.inspect import is_not_coroutine_function
from kaflow.dependencies import Scopes
from kaflow.exceptions import KaflowDeserializationException
from kaflow.message import Message

if TYPE_CHECKING:
    from aiokafka.abc import AbstractTokenProvider
    from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor

    from kaflow.asyncapi.models import AsyncAPI
    from kaflow.serializers import Serializer
    from kaflow.typing import TopicValueKeyHeader

ConsumerFunc = Callable[..., Union[Message, Awaitable[Union[Message, None]], None]]
ProducerFunc = Callable[..., Union[Message, Awaitable[Message]]]
ExceptionHandlerFunc = Callable[[Exception], Awaitable]
DeserializationErrorHandlerFunc = Callable[[KaflowDeserializationException], Awaitable]


class Kaflow:
    def __init__(
        self,
        brokers: str | list[str],
        client_id: str | None = None,
        group_id: str | None = None,
        acks: Literal[0, 1, "all"] = 1,
        compression_type: Literal["gzip", "snappy", "lz4", "zstd", None] = None,
        max_batch_size: int = 16384,
        partitioner: Callable[
            [bytes, list[int], list[int]], int
        ] = DefaultPartitioner(),
        max_request_size: int = 1048576,
        linger_ms: int = 0,
        send_backoff_ms: int = 100,
        connections_max_idle_ms: int = 540000,
        enable_idempotence: bool = False,
        transactional_id: str | None = None,
        transaction_timeout_ms: int = 60000,
        fetch_max_wait_ms: int = 500,
        fetch_max_bytes: int = 52428800,
        fetch_min_bytes: int = 1,
        max_partition_fetch_bytes: int = 1 * 1024 * 1024,
        request_timeout_ms: int = 40 * 1000,
        retry_backoff_ms: int = 100,
        auto_offset_reset: Literal["earliest", "latest", "none"] = "latest",
        enable_auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
        check_crcs: bool = True,
        metadata_max_age_ms: int = 5 * 60 * 1000,
        partition_assignment_strategy: list[AbstractPartitionAssignor] | None = None,
        max_poll_interval_ms: int = 300000,
        rebalance_timeout_ms: int | None = None,
        session_timeout_ms: int = 10000,
        heartbeat_interval_ms: int = 3000,
        consumer_timeout_ms: int = 200,
        max_poll_records: int | None = None,
        kafka_api_version: str = "auto",
        security_protocol: Literal[
            "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"
        ] = "PLAINTEXT",
        exclude_internal_topics: bool = True,
        connection_max_idle_ms: int = 540000,
        isolation_level: Literal[
            "read_committed", "read_uncommitted"
        ] = "read_committed",
        cafile: str | None = None,
        capath: str | None = None,
        cadata: bytes | None = None,
        certfile: str | None = None,
        keyfile: str | None = None,
        cert_password: str | None = None,
        sasl_mechanism: Literal[
            "PLAIN", "GSSAPI", "OAUTHBEARER", "SCRAM-SHA-256", "SCRAM-SHA-512"
        ]
        | None = None,
        sasl_plain_username: str | None = None,
        sasl_plain_password: str | None = None,
        sasl_kerberos_service_name: str = "kafka",
        sasl_kerberos_domain_name: str | None = None,
        sasl_oauth_token_provider: AbstractTokenProvider | None = None,
        lifespan: Callable[..., AsyncContextManager[None]] | None = None,
        asyncapi_version: str = "2.6.0",
        title: str = "Kaflow",
        version: str = "0.0.1",
        description: str | None = None,
        terms_of_service: str | None = None,
        contact: dict[str, str | Any] | None = None,
        license_info: dict[str, str | Any] | None = None,
    ) -> None:
        # AIOKafka
        self.brokers = brokers
        self.client_id = client_id or f"kaflow-{uuid4()}"
        self.group_id = group_id
        self.acks = acks
        self.compression_type = compression_type
        self.max_batch_size = max_batch_size
        self.partitioner = partitioner
        self.max_request_size = max_request_size
        self.linger_ms = linger_ms
        self.send_backoff_ms = send_backoff_ms
        self.connections_max_idle_ms = connections_max_idle_ms
        self.enable_idempotence = enable_idempotence
        self.transactional_id = transactional_id
        self.transaction_timeout_ms = transaction_timeout_ms
        self.fetch_max_wait_ms = fetch_max_wait_ms
        self.fetch_max_bytes = fetch_max_bytes
        self.fetch_min_bytes = fetch_min_bytes
        self.max_partition_fetch_bytes = max_partition_fetch_bytes
        self.request_timeout_ms = request_timeout_ms
        self.retry_backoff_ms = retry_backoff_ms
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.auto_commit_interval_ms = auto_commit_interval_ms
        self.check_crcs = check_crcs
        self.metadata_max_age_ms = metadata_max_age_ms
        self.partition_assignment_strategy = partition_assignment_strategy or (
            RoundRobinPartitionAssignor,
        )
        self.max_poll_interval_ms = max_poll_interval_ms
        self.rebalance_timeout_ms = rebalance_timeout_ms
        self.session_timeout_ms = session_timeout_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.consumer_timeout_ms = consumer_timeout_ms
        self.max_poll_records = max_poll_records
        self.kafka_api_version = kafka_api_version
        self.security_protocol = security_protocol
        self.exclude_internal_topics = exclude_internal_topics
        self.connection_max_idle_ms = connection_max_idle_ms
        self.isolation_level = isolation_level
        self.cafile = cafile
        self.capath = capath
        self.cadata = cadata
        self.certfile = certfile
        self.keyfile = keyfile
        self.cert_password = cert_password
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        self.sasl_kerberos_service_name = sasl_kerberos_service_name
        self.sasl_kerberos_domain_name = sasl_kerberos_domain_name
        self.sasl_oauth_token_provider = sasl_oauth_token_provider

        if security_protocol == "SSL":
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

        # di
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
            bootstrap_servers=self.brokers,
            client_id=self.client_id,
            group_id=self.group_id,
            fetch_min_bytes=self.fetch_min_bytes,
            fetch_max_bytes=self.fetch_max_bytes,
            fetch_max_wait_ms=self.fetch_max_wait_ms,
            max_partition_fetch_bytes=self.max_partition_fetch_bytes,
            max_poll_records=self.max_poll_records,
            request_timeout_ms=self.request_timeout_ms,
            retry_backoff_ms=self.retry_backoff_ms,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=self.enable_auto_commit,
            auto_commit_interval_ms=self.auto_commit_interval_ms,
            check_crcs=self.check_crcs,
            metadata_max_age_ms=self.metadata_max_age_ms,
            partition_assignment_strategy=self.partition_assignment_strategy,
            max_poll_interval_ms=self.max_poll_interval_ms,
            rebalance_timeout_ms=self.rebalance_timeout_ms,
            session_timeout_ms=self.session_timeout_ms,
            heartbeat_interval_ms=self.heartbeat_interval_ms,
            consumer_timeout_ms=self.consumer_timeout_ms,
            api_version=self.kafka_api_version,
            security_protocol=self.security_protocol,
            ssl_context=self.ssl_context,
            exclude_internal_topics=self.exclude_internal_topics,
            connections_max_idle_ms=self.connections_max_idle_ms,
            isolation_level=self.isolation_level,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            sasl_kerberos_domain_name=self.sasl_kerberos_domain_name,
            sasl_kerberos_service_name=self.sasl_kerberos_service_name,
            sasl_oauth_token_provider=self.sasl_oauth_token_provider,
        )

    def _create_producer(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=self.brokers,
            client_id=self.client_id,
            metadata_max_age_ms=self.metadata_max_age_ms,
            request_timeout_ms=self.request_timeout_ms,
            api_version=self.kafka_api_version,
            acks=self.acks,
            compression_type=self.compression_type,
            max_batch_size=self.max_batch_size,
            partitioner=self.partitioner,
            max_request_size=self.max_request_size,
            linger_ms=self.linger_ms,
            send_backoff_ms=self.send_backoff_ms,
            retry_backoff_ms=self.retry_backoff_ms,
            security_protocol=self.security_protocol,
            ssl_context=self.ssl_context,
            connections_max_idle_ms=self.connections_max_idle_ms,
            enable_idempotence=self.enable_idempotence,
            transactional_id=self.transactional_id,
            transaction_timeout_ms=self.transaction_timeout_ms,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            sasl_kerberos_service_name=self.sasl_kerberos_service_name,
            sasl_kerberos_domain_name=self.sasl_kerberos_domain_name,
            sasl_oauth_token_provider=self.sasl_oauth_token_provider,
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
            ) = parameters.get_function_parameters_info(func)
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
                    raise ValueError(
                        "Kaflow producer function has to return an instance of"
                        " `Message` containing the information of the message to be"
                        f" send. Update `{func.__name__}` to return a `Message`"
                        " instance."
                    )
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
            if is_not_coroutine_function(func):
                self._deserialization_error_handler = asyncify(func)
            return func

        return register_deserialization_error_handler

    def asyncapi(self) -> AsyncAPI:
        # if not self.asyncapi_schema:
        #     self.asyncapi_schema = build_asyncapi(
        #         asyncapi_version=self.asyncapi_version,
        #         title=self.title,
        #         version=self.version,
        #         description=self.description,
        #         terms_of_service=self.terms_of_service,
        #         contact=self.contact,
        #         license_info=self.license_info,
        #         consumers=self._consumers,
        #         producers=self._producers,
        #     )
        # return self.asyncapi_schema
        raise NotImplementedError("AsyncAPI is not implemented yet.")

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
