from __future__ import annotations

import asyncio
import contextvars
import functools
from typing import Awaitable, Callable, TypeVar

from typing_extensions import ParamSpec

P = ParamSpec("P")
R = TypeVar("R")


# Inspired by https://github.com/tiangolo/asyncer
def asyncify(func: Callable[P, R]) -> Callable[P, Awaitable[R]]:
    """A decorator to convert a synchronous function into an asynchronous one.

    Args:
        func: The synchronous function to convert.

    Returns:
        The asynchronous function.
    """

    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        # TODO: update to use `asyncio.to_thread`, once Python 3.8 is deprecated
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        func_call = functools.partial(ctx.run, func, *args, **kwargs)
        return await loop.run_in_executor(None, func_call)  # type: ignore

    return wrapper
