from __future__ import annotations

from typing import Any, NamedTuple, Union

from kaflow.typing import TopicValueKeyHeader


class ReadMessage(NamedTuple):
    value: Union[TopicValueKeyHeader, None] = None
    key: Union[TopicValueKeyHeader, None] = None
    headers: Union[dict[str, Any], None] = None
    offset: Union[int, None] = None
    partition: Union[int, None] = None
    timestamp: Union[int, None] = None


class Message(NamedTuple):
    value: Union[bytes, None] = None
    key: Union[bytes, None] = None
    headers: Union[dict[str, bytes], None] = None
    offset: Union[int, None] = None
    partition: Union[int, None] = None
    timestamp: Union[int, None] = None
