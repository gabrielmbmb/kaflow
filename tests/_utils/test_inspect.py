import inspect
from typing import Any, Callable

import pytest
from typing_extensions import Annotated

from kaflow._utils.inspect import (
    annotated_param_with,
    has_return_annotation,
    is_annotated_param,
    is_not_coroutine_function,
)


def func_with_none_return_annotation() -> None:
    pass


def func_with_no_return_annotation():
    pass


def func_with_return_annotation() -> int:
    pass


async def coroutine() -> None:
    pass


@pytest.mark.parametrize(
    "param, expected", [(Annotated[int, "extra", "metadata"], True), (int, False)]
)
def test_is_annotated_param_true(param: Any, expected: bool) -> None:
    assert is_annotated_param(param) == expected


class DummyUnitTest:
    pass


@pytest.mark.parametrize(
    "param, annotated_with, expected",
    [
        (Annotated[int, "magic_unit_test_flag"], "magic_unit_test_flag", True),
        (Annotated[int, "extra", "metadata"], "magic_unit_test_flag", False),
        (Annotated[int, DummyUnitTest()], DummyUnitTest, True),
    ],
)
def test_annotated_param_with(param: Any, annotated_with: Any, expected: bool) -> None:
    assert annotated_param_with(annotated_with, param) == expected


@pytest.mark.parametrize(
    "func, expected",
    [
        (func_with_none_return_annotation, False),
        (func_with_no_return_annotation, False),
        (func_with_return_annotation, True),
    ],
)
def test_has_return_annotation(func: Callable[..., Any], expected: bool) -> None:
    signature = inspect.signature(func)
    assert has_return_annotation(signature) == expected


@pytest.mark.parametrize(
    "func, expected", [(coroutine, False), (func_with_return_annotation, True)]
)
def test_is_not_coroutine_function(func, expected: bool) -> None:
    assert is_not_coroutine_function(func) == expected
