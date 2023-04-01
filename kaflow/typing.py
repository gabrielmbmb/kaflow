from __future__ import annotations

from typing import Awaitable, Callable, Union

from aiokafka import ConsumerRecord
from pydantic import BaseModel

ConsumerFuncR = Union[BaseModel, None]
ConsumerFunc = Callable[..., Union[ConsumerFuncR, Awaitable[ConsumerFuncR]]]
ProducerFunc = Callable[..., Union[BaseModel, Awaitable[BaseModel]]]
ExceptionHandlerFunc = Callable[..., Union[None, Awaitable[None]]]
DeserializationErrorHandlerFunc = Callable[[Exception, ConsumerRecord], Awaitable[None]]
