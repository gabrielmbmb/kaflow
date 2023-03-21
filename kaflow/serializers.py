import json
from typing import Annotated, Any, Protocol, TypeVar

from pydantic import BaseModel

try:
    import fastavro
except ImportError:
    fastavro = None


class Serializer(Protocol):
    @staticmethod
    def serialize(data: BaseModel) -> bytes:
        ...

    @staticmethod
    def deserialize(data: bytes) -> Any:
        ...


class JsonSerializer(Serializer):
    @staticmethod
    def serialize(data: BaseModel) -> bytes:
        return data.json().encode()

    @staticmethod
    def deserialize(data: bytes) -> Any:
        return json.loads(data)


class AvroSerializer(Serializer):
    @staticmethod
    def serialize(data: BaseModel) -> bytes:
        return fastavro.schemaless_writer(data)

    @staticmethod
    def deserialize(data: bytes) -> Any:
        return fastavro.schemaless_reader(data)


class FastAvroSerializer(Serializer):
    pass


MessageSerializerFlag = "MessageSerializer"

_BaseModelT = TypeVar("_BaseModelT", bound=BaseModel)
Json = Annotated[_BaseModelT, JsonSerializer, MessageSerializerFlag]
Avro = Annotated[_BaseModelT, AvroSerializer, MessageSerializerFlag]
