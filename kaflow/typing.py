from __future__ import annotations

from typing import Awaitable, Callable, Union

from pydantic import BaseModel

TopicFuncR = Union[BaseModel, None]

TopicFunc = Callable[..., TopicFuncR | Awaitable[TopicFuncR]]
