from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Awaitable, Callable, TypeVar

from typing_extensions import Annotated, ParamSpec, TypeGuard, get_args, get_origin

if TYPE_CHECKING:
    from inspect import Signature


def is_annotated_param(param: Any) -> bool:
    return get_origin(param) is Annotated


def annotated_param_with(item: Any, param: Any) -> bool:
    if is_annotated_param(param):
        for arg in get_args(param):
            if arg == item or (type(item) == type and isinstance(arg, item)):
                return True
    return False


def has_return_annotation(signature: Signature) -> bool:
    return (
        signature.return_annotation is not None
        and signature.return_annotation != inspect.Signature.empty
    )


P = ParamSpec("P")
R = TypeVar("R")


def is_not_coroutine_function(
    func: Callable[P, R | Awaitable[R]]
) -> TypeGuard[Callable[P, R]]:
    """Check if a function is not a coroutine function. This function narrows the type
    of the function to a synchronous function.

    Args:
        func: The function to check.

    Returns:
        `True` if the function is not a coroutine function, `False` otherwise.
    """
    return not inspect.iscoroutinefunction(func)
