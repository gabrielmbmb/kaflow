from __future__ import annotations

import asyncio
import contextvars
import functools
import sys
from typing import Any, Awaitable, Callable, Coroutine, TypeVar

from kaflow._utils.typing import ParamSpec


async def task_group(
    funcs: list[Callable[..., Coroutine[Any, Any, None]]], *args: Any, **kwargs: Any
) -> None:
    """A convenient function to run a list of coroutines. It will check the Python
    version to see if the new `asyncio.TaskGroup` and exception groups introduced in
    Python 3.11 are available. If they are, it will use them. Otherwise, it will use
    the old `asyncio.gather` function.

    Args:
        funcs: A list of coroutines to run.
        *args: Positional arguments to pass to the coroutines.
        **kwargs: Keyword arguments to pass to the coroutines.

    Raises:
        Exception: If any of the coroutines raise an exception, it will be raised
            here.
    """
    if sys.version_info < (3, 11):
        try:
            await asyncio.gather(*[func(*args, **kwargs) for func in funcs])
        except Exception as e:
            raise Exception("An exception ocurred while running coroutines") from e
    else:
        try:
            async with asyncio.TaskGroup() as tg:
                for func in funcs:
                    tg.create_task(func(*args, **kwargs))
        except ExceptionGroup as eg:
            raise Exception("An exception ocurred while running coroutines") from eg


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
