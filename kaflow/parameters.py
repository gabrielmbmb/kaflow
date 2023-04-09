from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, TypeVar

from di.dependent import Dependent, Marker
from typing_extensions import Annotated

from kaflow._utils.inspect import annotated_param_with
from kaflow.message import ReadMessage
from kaflow.serializers import Serializer

if TYPE_CHECKING:
    from kaflow.applications import ConsumerFunc
    from kaflow.typing import TopicValueKeyHeader

FROM_VALUE_FLAG = "from_value"
FROM_KEY_FLAG = "from_key"
FROM_HEADER_FLAG = "from_header"


def _get_serializer_info(param: Any) -> tuple[type[Serializer] | None, dict[str, Any]]:
    serializer: type[Serializer] | None = None
    serializer_extra: dict[str, Any] = {}
    for item in param.__metadata__:
        if inspect.isclass(item) and issubclass(item, Serializer):
            serializer = item
        elif isinstance(item, dict):
            serializer_extra = item
    return serializer, serializer_extra


def _annotated_serializer_info(
    param: tuple[str, Any], func_name: str
) -> tuple[type[TopicValueKeyHeader], type[Serializer] | None, dict[str, Any]]:
    """Get the type and serializer of a parameter annotated (`Annotated[...]`) with
    a Kaflow serializer. This function expect the parameter to be `Annotated` with a
    Kaflow serializer, the `kaflow.serializers.MESSAGE_SERIALIZER_FLAG`, and optionally
    with a `dict` containing extra information that could be used by the serializer.

    Args:
        param: the annotated parameter to get the type and serializer from.
        func_name: the name of the function that contains the annotated parameter.

    Returns:
        A tuple with the type and serializer of the annotated parameter, and a `dict`
        containing extra information that could be used by the serializer.
    """
    param_type = param[1].__args__[0]
    serializer, serializer_extra = _get_serializer_info(param[1])
    if not serializer and param_type is not bytes:
        raise ValueError(
            f"'{param[0]}' parameter of '{func_name}' function has not been annotated"
            " with a Kaflow serializer and its type is not `bytes`. Please annotate"
            " the parameter with a Kaflow serializer or use `bytes` as the type."
        )
    return param_type, serializer, serializer_extra


def _serializer_info(
    param: tuple[str, Any], func_name: str
) -> tuple[type[TopicValueKeyHeader], Serializer | None]:
    param_type, serializer, serializer_extra = _annotated_serializer_info(
        param, func_name
    )
    if serializer:
        return param_type, serializer(**serializer_extra)
    return param_type, None


def _get_params(signature: inspect.Signature) -> dict[str, list[tuple[str, Any]]]:
    params: dict[str, list[Any]] = {
        FROM_VALUE_FLAG: [],
        FROM_KEY_FLAG: [],
        FROM_HEADER_FLAG: [],
    }
    for param in signature.parameters.values():
        if annotated_param_with(Value, param.annotation):
            params[FROM_VALUE_FLAG].append((param.name, param.annotation))
        elif annotated_param_with(Key, param.annotation):
            params[FROM_KEY_FLAG].append((param.name, param.annotation))
        elif annotated_param_with(Header, param.annotation):
            params[FROM_HEADER_FLAG].append((param.name, param.annotation))
    return params


def _get_from_value_param_info(
    params: dict[str, list[tuple[str, Any]]], func_name: str
) -> tuple[type[TopicValueKeyHeader], Serializer | None]:
    if not params[FROM_VALUE_FLAG]:
        raise ValueError(
            f"'{func_name}' function does not have a parameter annotated"
            " `FromValue` to receive the value of the message from the topic."
        )
    if len(params[FROM_VALUE_FLAG]) > 1:
        raise ValueError(
            f"'{func_name}' function has more than one parameter"
            " annotated `FromValue`. Only one parameter can be annotated"
            " `FromValue` to receive the value of the message from the topic."
        )
    return _serializer_info(params[FROM_VALUE_FLAG][0], func_name)


def _get_from_key_param_info(
    params: dict[str, list[tuple[str, Any]]], func_name: str
) -> tuple[type[TopicValueKeyHeader] | None, Serializer | None]:
    key_param_type: type[TopicValueKeyHeader] | None = None
    key_serializer: Serializer | None = None
    if params[FROM_KEY_FLAG]:
        key_param_type, key_serializer = _serializer_info(
            param=params[FROM_KEY_FLAG][0], func_name=func_name
        )
    return key_param_type, key_serializer


def _get_from_headers_param_info(
    params: dict[str, list[Any]], func_name: str
) -> dict[str, tuple[type[TopicValueKeyHeader], Serializer | None]] | None:
    headers = {}
    for header_param in params[FROM_HEADER_FLAG]:
        name, _ = header_param
        (header_param_type, header_serializer) = _serializer_info(
            header_param, func_name
        )
        headers[name] = (header_param_type, header_serializer)
    if headers:
        return headers
    return None


def get_function_parameters_info(
    func: ConsumerFunc,
) -> tuple[
    type[TopicValueKeyHeader],
    Serializer | None,
    type[TopicValueKeyHeader] | None,
    Serializer | None,
    dict[str, tuple[type[TopicValueKeyHeader], Serializer | None]] | None,
]:
    signature = inspect.signature(func)
    params = _get_params(signature)
    value_param_type, value_serializer = _get_from_value_param_info(
        params=params, func_name=func.__name__
    )
    key_param_type, key_serializer = _get_from_key_param_info(
        params=params, func_name=func.__name__
    )
    headers_type_serializers = _get_from_headers_param_info(
        params=params, func_name=func.__name__
    )
    return (
        value_param_type,
        value_serializer,
        key_param_type,
        key_serializer,
        headers_type_serializers,
    )


class _MessageAttr(Marker):
    def __init__(self, attr_name: str) -> None:
        self.attr_name = attr_name

    def register_parameter(self, param: inspect.Parameter) -> Dependent[Any]:
        def get_value(message: Annotated[ReadMessage, Marker(scope="consumer")]) -> Any:
            return getattr(message, self.attr_name)

        return Dependent(get_value, scope="consumer", use_cache=False)


class Value(_MessageAttr):
    def __init__(self) -> None:
        super().__init__("value")


class Key(_MessageAttr):
    def __init__(self) -> None:
        super().__init__("key")


class Header(Marker):
    def __init__(self, alias: str | None = None) -> None:
        self.alias = alias
        super().__init__(call=None, scope="consumer", use_cache=False)

    def register_parameter(self, param: inspect.Parameter) -> Dependent[Any]:
        if self.alias:
            name = self.alias
        else:
            name = param.name

        def get_header(
            message: Annotated[ReadMessage, Marker(scope="consumer")]
        ) -> Any:
            if message.headers:
                return message.headers.get(name)
            return None

        return Dependent(get_header, scope="consumer")


class Partition(_MessageAttr):
    def __init__(self) -> None:
        super().__init__("partition")


class Timestamp(_MessageAttr):
    def __init__(self) -> None:
        super().__init__("timestamp")


class Offset(_MessageAttr):
    def __init__(self) -> None:
        super().__init__("offset")


_T = TypeVar("_T")
FromValue = Annotated[_T, Value()]
FromKey = Annotated[_T, Key()]
FromHeader = Annotated[_T, Header()]
MessageOffset = Annotated[int, Offset()]
MessagePartition = Annotated[int, Partition()]
MessageTimestamp = Annotated[int, Timestamp()]
