from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast

from pydantic import BaseModel

from kaflow._utils.typing import Annotated

if TYPE_CHECKING:
    from kaflow.typing import TopicMessage

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

T = TypeVar("T")


def _serialize(message: TopicMessage, serializer: Serializer) -> bytes:
    if isinstance(message, BaseModel):
        message = message.dict()
    return serializer.serialize(message)


class Serializer(Protocol):
    kwargs: dict[str, Any]

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def serialize(self, data: Any) -> bytes:
        ...

    def deserialize(self, data: bytes) -> Any:
        ...

    @staticmethod
    def extra_annotations_keys() -> list[str]:
        return []


class JsonSerializer(Serializer):
    def serialize(self, data: Any) -> bytes:
        return json.dumps(data).encode()

    def deserialize(self, data: bytes) -> Any:
        return json.loads(data)


Json = Annotated[T, JsonSerializer, MESSAGE_SERIALIZER_FLAG]


if has_fastavro:

    class AvroSerializer(Serializer):
        def serialize(self, data: Any) -> bytes:
            schema = self.kwargs.get("avro_schema")
            assert schema is not None, "avro_schema is required"
            bytes_io = io.BytesIO()
            fastavro.schemaless_writer(bytes_io, schema, data)
            return bytes_io.getvalue()

        def deserialize(self, data: bytes) -> Any:
            schema = self.kwargs.get("avro_schema")
            assert schema is not None, "avro_schema is required"
            return fastavro.schemaless_reader(io.BytesIO(data), schema)

        @staticmethod
        def extra_annotations_keys() -> list[str]:
            return ["avro_schema"]

    Avro = Annotated[T, AvroSerializer, MESSAGE_SERIALIZER_FLAG]

if has_protobuf:

    class ProtobufSerializer(Serializer):
        def serialize(self, data: Any) -> bytes:
            protobuf_schema = self.kwargs.get("protobuf_schema")
            assert protobuf_schema is not None, "protobuf_schema is required"
            entity = protobuf_schema()
            for key, value in data.items():
                setattr(entity, key, value)
            return cast(bytes, entity.SerializeToString())

        def deserialize(self, data: bytes) -> Any:
            protobuf_schema = self.kwargs.get("protobuf_schema")
            assert protobuf_schema is not None, "protobuf_schema is required"
            entity = protobuf_schema()
            entity.ParseFromString(data)
            return json_format.MessageToDict(entity)

        @staticmethod
        def extra_annotations_keys() -> list[str]:
            return ["protobuf_schema"]

    Protobuf = Annotated[T, ProtobufSerializer, MESSAGE_SERIALIZER_FLAG]
