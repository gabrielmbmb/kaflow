from __future__ import annotations

from kaflow.serializers import ProtobufSerializer
from tests.key_value_pb2 import KeyValue


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


def test_protobuf_required_extra_annotation_keys() -> None:
    assert ProtobufSerializer.required_extra_annotations_keys() == ["protobuf_schema"]
