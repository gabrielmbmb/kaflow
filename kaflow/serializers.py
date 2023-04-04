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
    def serialize(self, data: Any) -> bytes:
        ...

    def deserialize(self, data: bytes) -> Any:
        ...

    @staticmethod
    def extra_annotations_keys() -> list[str]:
        return []


class JsonSerializer(Serializer):
    def __init__(self, **kwargs: Any) -> None:
        pass

    def serialize(self, data: Any) -> bytes:
        return json.dumps(data).encode()

    def deserialize(self, data: bytes) -> Any:
        return json.loads(data)


Json = Annotated[T, JsonSerializer, MESSAGE_SERIALIZER_FLAG]


if has_fastavro:

    class AvroSerializer(Serializer):
        def __init__(self, avro_schema: dict[str, Any], **kwargs: Any) -> None:
            self.avro_schema = avro_schema

        def serialize(self, data: Any) -> bytes:
            bytes_io = io.BytesIO()
            fastavro.schemaless_writer(bytes_io, self.avro_schema, data)
            return bytes_io.getvalue()

        def deserialize(self, data: bytes) -> Any:
            return fastavro.schemaless_reader(io.BytesIO(data), self.avro_schema)

        @staticmethod
        def extra_annotations_keys() -> list[str]:
            return ["avro_schema"]

    Avro = Annotated[T, AvroSerializer, MESSAGE_SERIALIZER_FLAG]

if has_protobuf:

    class ProtobufSerializer(Serializer):
        def __init__(self, protobuf_schema: type[Any], **kwargs: Any) -> None:
            self.protobuf_schema = protobuf_schema

        def serialize(self, data: Any) -> bytes:
            entity = self.protobuf_schema()
            for key, value in data.items():
                setattr(entity, key, value)
            return cast(bytes, entity.SerializeToString())

        def deserialize(self, data: bytes) -> Any:
            entity = self.protobuf_schema()
            entity.ParseFromString(data)
            return json_format.MessageToDict(entity)

        @staticmethod
        def extra_annotations_keys() -> list[str]:
            return ["protobuf_schema"]

    Protobuf = Annotated[T, ProtobufSerializer, MESSAGE_SERIALIZER_FLAG]
