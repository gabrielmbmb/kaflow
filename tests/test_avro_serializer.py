from __future__ import annotations

from kaflow.serializers import AvroSerializer


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


def test_avro_serializer_serialize_with_schema() -> None:
    serializer = AvroSerializer(
        include_schema=True,
        avro_schema={
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "key", "type": "string"},
                {"name": "value", "type": "string"},
            ],
        },
        # Need to specify sync_marker to make the test deterministic
        sync_marker=b"\x80ak.\x85\x8d\x85\xd1\xc2$\x01\xf8\xa3Dy\x1b",
    )
    assert (
        serializer.serialize({"key": "unit_test_key", "value": "unit_test_value"})
        == b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xec\x01{"type": "record",'
        b' "name": "test", "fields": [{"name": "key", "type": "string"}, {"name":'
        b' "value", "type":'
        b' "string"}]}\x00\x80ak.\x85\x8d\x85\xd1\xc2$\x01\xf8\xa3Dy\x1b\x02<\x1aunit_test_key\x1eunit_test_value\x80ak.\x85\x8d\x85\xd1\xc2$\x01\xf8\xa3Dy\x1b'
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


def test_avro_serializer_deserialize_with_schema() -> None:
    serializer = AvroSerializer()
    assert serializer.deserialize(
        b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xec\x01{"type": "record",'
        b' "name": "test", "fields": [{"name": "key", "type": "string"}, {"name":'
        b' "value", "type":'
        b' "string"}]}\x00\x80ak.\x85\x8d\x85\xd1\xc2$\x01\xf8\xa3Dy\x1b\x02<\x1aunit_test_key\x1eunit_test_value\x80ak.\x85\x8d\x85\xd1\xc2$\x01\xf8\xa3Dy\x1b'
    ) == {
        "key": "unit_test_key",
        "value": "unit_test_value",
    }
