from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from kaflow._utils.typing import Annotated, get_args, get_origin

if TYPE_CHECKING:
    from inspect import Signature


def is_annotated_param(param: Any) -> bool:
    return get_origin(param) is Annotated


def annotated_param_with(item: Any, param: Any) -> bool:
    return is_annotated_param(param) and item in get_args(param)


def signature_contains_annotated_param_with(
    item: Any, signature: Signature
) -> inspect.Parameter | None:
    for param in signature.parameters.values():
        if annotated_param_with(item, param.annotation):
            if item in param.annotation.__metadata__:
                return param
    return None


def has_return_annotation(signature: Signature) -> bool:
    return (
        signature.return_annotation
        and signature.return_annotation != inspect.Signature.empty
    )
