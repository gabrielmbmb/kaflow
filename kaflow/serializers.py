from __future__ import annotations

import io
import json
from typing import Any, Protocol, TypeVar, cast

from pydantic import BaseModel

from kaflow._utils.typing import Annotated

try:
    import fastavro

    has_fastavro = True
except ImportError:
    has_fastavro = False

try:
    from google.protobuf import json_format

    has_protobuf = True
except ImportError:
    has_protobuf = False

MESSAGE_SERIALIZER_FLAG = "MessageSerializer"

_BaseModelT = TypeVar("_BaseModelT", bound=BaseModel)


class Serializer(Protocol):
    @staticmethod
    def serialize(data: BaseModel, **kwargs: Any) -> bytes:
        ...

    @staticmethod
    def deserialize(data: bytes, **kwargs: Any) -> Any:
        ...

    @staticmethod
    def extra_annotations_keys() -> list[str]:
        return []


class JsonSerializer(Serializer):
    @staticmethod
    def serialize(data: BaseModel, **kwargs: Any) -> bytes:
        return data.json().encode()

    @staticmethod
    def deserialize(data: bytes, **kwargs: Any) -> Any:
        return json.loads(data)


Json = Annotated[_BaseModelT, JsonSerializer, MESSAGE_SERIALIZER_FLAG]


if has_fastavro:

    class AvroSerializer(Serializer):
        @staticmethod
        def serialize(data: BaseModel, **kwargs: Any) -> bytes:
            schema = kwargs.get("avro_schema")
            assert schema is not None, "avro_schema is required"
            bytes_io = io.BytesIO()
            fastavro.schemaless_writer(bytes_io, schema, data.dict())
            return bytes_io.getvalue()

        @staticmethod
        def deserialize(data: bytes, **kwargs: Any) -> Any:
            schema = kwargs.get("avro_schema")
            assert schema is not None, "avro_schema is required"
            return fastavro.schemaless_reader(io.BytesIO(data), schema)

        @staticmethod
        def extra_annotations_keys() -> list[str]:
            return ["avro_schema"]

    Avro = Annotated[_BaseModelT, AvroSerializer, MESSAGE_SERIALIZER_FLAG]

if has_protobuf:

    class ProtobufSerializer(Serializer):
        @staticmethod
        def serialize(data: BaseModel, **kwargs: Any) -> bytes:
            protobuf_schema = kwargs.get("protobuf_schema")
            assert protobuf_schema is not None, "protobuf_schema is required"
            entity = protobuf_schema()
            for key, value in data.dict().items():
                setattr(entity, key, value)
            return cast(bytes, entity.SerializeToString())

        @staticmethod
        def deserialize(data: bytes, **kwargs: Any) -> Any:
            protobuf_schema = kwargs.get("protobuf_schema")
            assert protobuf_schema is not None, "protobuf_schema is required"
            entity = protobuf_schema()
            entity.ParseFromString(data)
            return json_format.MessageToDict(entity)

        @staticmethod
        def extra_annotations_keys() -> list[str]:
            return ["protobuf_schema"]

    Protobuf = Annotated[_BaseModelT, ProtobufSerializer, MESSAGE_SERIALIZER_FLAG]
