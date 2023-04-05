from __future__ import annotations

from pydantic import BaseModel

from kaflow.serializers import (
    AvroSerializer,
    JsonSerializer,
    ProtobufSerializer,
    Serializer,
)
from tests.key_value_pb2 import KeyValue


class KeyValueModel(BaseModel):
    key: str
    value: str


def test_serializer_extra_annotation_keys() -> None:
    assert Serializer.extra_annotations_keys() == []


def test_json_serializer_serialize() -> None:
    serializer = JsonSerializer()
    assert serializer.serialize({"key": "value"}) == b'{"key": "value"}'


def test_json_serializer_deserialize() -> None:
    serializer = JsonSerializer()
    assert serializer.deserialize(b'{"key": "value"}') == {"key": "value"}


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
