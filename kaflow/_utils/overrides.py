# Taken from: https://github.com/adriangb/xpresso/blob/main/xpresso/_utils/overrides.py
from __future__ import annotations

import contextlib
import inspect
import typing
from types import TracebackType
from typing import cast

from di import Container
from di.api.dependencies import DependentBase
from di.api.providers import DependencyProvider
from di.dependent import Dependent
from typing_extensions import get_args

from kaflow._utils.inspect import is_annotated_param


def get_type(param: inspect.Parameter) -> type:
    if is_annotated_param(param):
        type_ = next(iter(get_args(param.annotation)))
    else:
        type_ = param.annotation
    return cast(type, type_)


class DependencyOverrideManager:
    _stacks: typing.List[contextlib.ExitStack]

    def __init__(self, container: Container) -> None:
        self._container = container
        self._stacks = []

    def __setitem__(
        self, target: DependencyProvider, replacement: DependencyProvider
    ) -> None:
        def hook(
            param: typing.Optional[inspect.Parameter],
            dependent: DependentBase[typing.Any],
        ) -> typing.Optional[DependentBase[typing.Any]]:
            if not isinstance(dependent, Dependent):
                return None
            scope = dependent.scope
            dep = Dependent(
                replacement,
                scope=scope,
                use_cache=dependent.use_cache,
                wire=dependent.wire,
            )
            if param is not None and param.annotation is not param.empty:
                type_ = get_type(param)
                if type_ is target:
                    return dep
            if dependent.call is not None and dependent.call is target:
                return dep
            return None

        cm = self._container.bind(hook)
        if self._stacks:
            self._stacks[-1].enter_context(cm)

    def __enter__(self) -> DependencyOverrideManager:
        self._stacks.append(contextlib.ExitStack().__enter__())
        return self

    def __exit__(
        self,
        __exc_type: typing.Optional[typing.Type[BaseException]],
        __exc_value: typing.Optional[BaseException],
        __traceback: typing.Optional[TracebackType],
    ) -> typing.Optional[bool]:
        return self._stacks.pop().__exit__(__exc_type, __exc_value, __traceback)
