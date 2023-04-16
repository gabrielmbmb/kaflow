from __future__ import annotations

import asyncio
from functools import wraps
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from aiokafka import ConsumerRecord

if TYPE_CHECKING:
    from kaflow.applications import Kaflow
    from kaflow.message import Message


def intercept_publish(
    func: Callable[..., Awaitable[None]]
) -> Callable[..., Awaitable[None]]:
    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> None:
        pass

    return wrapper


class TestClient:
    """Test client for testing a `Kaflow` application."""

    def __init__(self, app: Kaflow) -> None:
        self.app = app
        self.app._publish = intercept_publish(self.app._publish)  # type: ignore
        self._loop = asyncio.get_event_loop()

    def publish(
        self,
        topic: str,
        partition: int,
        offset: int,
        timestamp: int,
        key: bytes,
        value: bytes,
        headers: dict[str, bytes],
    ) -> Message | None:
        record = ConsumerRecord(
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp=timestamp,
            timestamp_type=0,
            key=key,
            value=value,
            checksum=0,
            serialized_key_size=len(key),
            serialized_value_size=len(value),
            headers=headers,
        )

        async def _publish() -> Message | None:
            consumer = self.app._get_consumer(topic)
            async with self.app.lifespan():
                return await consumer.consume(record)

        return self._loop.run_until_complete(_publish())
