from __future__ import annotations

from typing import Awaitable, Callable

from pydantic import BaseModel

ConsumerFunc = Callable[..., Awaitable[BaseModel | None]]
