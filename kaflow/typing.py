from __future__ import annotations

from typing import Awaitable, Callable

from pydantic import BaseModel

ConsumerFunc = Callable[[BaseModel], Awaitable[BaseModel | None]]


MessageDeserializerFunc = Callable[[bytes], BaseModel]
