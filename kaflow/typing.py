from __future__ import annotations

from typing import Awaitable, Callable, Union

from aiokafka import ConsumerRecord
from pydantic import BaseModel

ConsumerFuncR = Union[BaseModel, None]
ConsumerFunc = Callable[..., ConsumerFuncR | Awaitable[ConsumerFuncR]]
ProducerFunc = Callable[..., BaseModel | Awaitable[BaseModel]]
ExceptionHandlerFunc = Callable[..., None | Awaitable[None]]
DeserializationErrorHandlerFunc = Callable[[Exception, ConsumerRecord], Awaitable[None]]
