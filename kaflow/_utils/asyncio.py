import asyncio
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
            raise Exception(
                f"An exception ocurred while running coroutines: {e}."
            ) from e
    else:
        try:
            async with asyncio.TaskGroup() as tg:
                for func in funcs:
                    tg.create_task(func(*args, **kwargs))
        except* Exception as eg:
            raise Exception from eg


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
        return await asyncio.to_thread(func, *args, **kwargs)

    return wrapper
