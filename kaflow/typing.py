from __future__ import annotations

from typing import Awaitable, Callable, Union

from aiokafka import ConsumerRecord
from pydantic import BaseModel

TopicMessage = Union[bytes, object, BaseModel]
ConsumerFuncR = Union[TopicMessage, None]
ConsumerFunc = Callable[..., Union[ConsumerFuncR, Awaitable[ConsumerFuncR]]]
ProducerFunc = Callable[..., Union[TopicMessage, Awaitable[TopicMessage]]]
ExceptionHandlerFunc = Callable[[Exception], Awaitable]
DeserializationErrorHandlerFunc = Callable[[Exception, ConsumerRecord], Awaitable[None]]
