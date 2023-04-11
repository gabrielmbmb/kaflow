from __future__ import annotations

from kaflow.serializers import StringSerializer


def test_string_serializer_serialize() -> None:
    serializer = StringSerializer()
    assert serializer.serialize("hello") == b"hello"


def test_string_serializer_deserialize() -> None:
    serializer = StringSerializer()
    assert serializer.deserialize(b"hello") == "hello"
