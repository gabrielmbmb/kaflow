from __future__ import annotations

from kaflow.serializers import JsonSerializer


def test_json_serializer_serialize() -> None:
    serializer = JsonSerializer()
    assert serializer.serialize({"key": "value"}) == b'{"key": "value"}'


def test_json_serializer_deserialize() -> None:
    serializer = JsonSerializer()
    assert serializer.deserialize(b'{"key": "value"}') == {"key": "value"}
