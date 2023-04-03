from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from kaflow.serializers import (
    AvroSerializer,
    JsonSerializer,
    ProtobufSerializer,
    Serializer,
    _serialize,
)
from tests.key_value_pb2 import KeyValue


class KeyValueModel(BaseModel):
    key: str
    value: str


def test_serializer_init() -> None:
    serializer = Serializer(
        extra_kwarg_1="extra_kwarg_1", extra_kwarg_2="extra_kwarg_2"
    )
    assert serializer.kwargs == {
        "extra_kwarg_1": "extra_kwarg_1",
        "extra_kwarg_2": "extra_kwarg_2",
    }


def test_serializer_extra_annotation_keys() -> None:
    assert Serializer.extra_annotations_keys() == []


def test_json_serializer_serialize() -> None:
    serializer = JsonSerializer()
    assert serializer.serialize({"key": "value"}) == b'{"key": "value"}'


def test_json_serializer_deserialize() -> None:
    serializer = JsonSerializer()
    assert serializer.deserialize(b'{"key": "value"}') == {"key": "value"}


def test_avro_serializer_serialize_raise_error() -> None:
    with pytest.raises(AssertionError, match="avro_schema is required"):
        serializer = AvroSerializer()
        serializer.serialize({"key": "value"})


def test_avro_serializer_deserialize_raise_error() -> None:
    with pytest.raises(AssertionError, match="avro_schema is required"):
        serializer = AvroSerializer()
        serializer.deserialize(b'{"key": "value"}')


def test_avro_serializer_serialize() -> None:
    serializer = AvroSerializer(
        avro_schema={
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "key", "type": "string"},
                {"name": "value", "type": "string"},
            ],
        }
    )
    assert (
        serializer.serialize({"key": "unit_test_key", "value": "unit_test_value"})
        == b"\x1aunit_test_key\x1eunit_test_value"
    )


def test_avro_serializer_deserialize() -> None:
    serializer = AvroSerializer(
        avro_schema={
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "key", "type": "string"},
                {"name": "value", "type": "string"},
            ],
        }
    )
    assert serializer.deserialize(b"\x1aunit_test_key\x1eunit_test_value") == {
        "key": "unit_test_key",
        "value": "unit_test_value",
    }


def test_avro_extra_annotation_keys() -> None:
    assert AvroSerializer.extra_annotations_keys() == ["avro_schema"]


def test_protobuf_serializer_serialize_raise_error() -> None:
    with pytest.raises(AssertionError, match="protobuf_schema is required"):
        serializer = ProtobufSerializer()
        serializer.serialize({"key": "value"})


def test_protobuf_serializer_deserialize_raise_error() -> None:
    with pytest.raises(AssertionError, match="protobuf_schema is required"):
        serializer = ProtobufSerializer()
        serializer.deserialize(b'{"key": "value"}')


def test_protobuf_serializer_serialize() -> None:
    serializer = ProtobufSerializer(protobuf_schema=KeyValue)
    assert (
        serializer.serialize({"key": "unit_test_key", "value": "unit_test_value"})
        == b"\n\runit_test_key\x12\x0funit_test_value"
    )


def test_protobuf_serializer_deserialize() -> None:
    serializer = ProtobufSerializer(protobuf_schema=KeyValue)
    assert serializer.deserialize(b"\n\runit_test_key\x12\x0funit_test_value") == {
        "key": "unit_test_key",
        "value": "unit_test_value",
    }


def test_protobuf_extra_annotation_keys() -> None:
    assert ProtobufSerializer.extra_annotations_keys() == ["protobuf_schema"]


@pytest.mark.parametrize(
    "message",
    [
        {"key": "unit_test_key", "value": "unit_test_value"},
        KeyValueModel(key="unit_test_key", value="unit_test_value"),
    ],
)
def test__serialize(message: dict[str, Any] | BaseModel) -> None:
    serializer = JsonSerializer()
    assert (
        _serialize(serializer=serializer, message=message)
        == b'{"key": "unit_test_key", "value": "unit_test_value"}'
    )
