from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord


class KaflowException(Exception):
    ...


class KaflowDeserializationException(KaflowException):
    def __init__(self, message: str, record: ConsumerRecord) -> None:
        super().__init__(message)
        self.record = record
