from __future__ import annotations

import io
import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar, cast

from typing_extensions import Annotated

if TYPE_CHECKING:
    pass

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


class Serializer(ABC):
    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        ...

    @staticmethod
    def extra_annotations_keys() -> list[str]:
        return []


class StringSerializer(Serializer):
    def __init__(self, **kwargs: Any) -> None:
        pass

    def serialize(self, data: Any) -> bytes:
        return str(data).encode()

    def deserialize(self, data: bytes) -> Any:
        return data.decode()


String = Annotated[T, StringSerializer, MESSAGE_SERIALIZER_FLAG]


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
        def __init__(
            self,
            avro_schema: dict[str, Any] | None = None,
            include_schema: bool = False,
            **kwargs: Any,
        ) -> None:
            self.avro_schema = avro_schema
            self.include_schema = include_schema

        def serialize(self, data: Any) -> bytes:
            bytes_io = io.BytesIO()
            if self.include_schema:
                fastavro.writer(bytes_io, self.avro_schema, [data])
            else:
                fastavro.schemaless_writer(bytes_io, self.avro_schema, data)
            return bytes_io.getvalue()

        def deserialize(self, data: bytes) -> Any:
            bytes_io = io.BytesIO(data)
            if self.avro_schema:
                return fastavro.schemaless_reader(io.BytesIO(data), self.avro_schema)
            return list(fastavro.reader(bytes_io))[0]

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
