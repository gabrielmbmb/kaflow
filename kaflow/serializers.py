import io
import json
from typing import Annotated, Any, Protocol, TypeVar

from pydantic import BaseModel

try:
    import fastavro
except ImportError:
    fastavro = None


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


class AvroSerializer(Serializer):
    @staticmethod
    def serialize(data: BaseModel, **kwargs: Any) -> bytes:
        schema = kwargs.get("avro_schema")
        bytes_io = io.BytesIO()
        fastavro.schemaless_writer(bytes_io, schema, data.dict())
        return bytes_io.getvalue()

    @staticmethod
    def deserialize(data: bytes, **kwargs: Any) -> Any:
        schema = kwargs.get("avro_schema")
        return fastavro.schemaless_reader(io.BytesIO(data), schema)

    @staticmethod
    def extra_annotations_keys() -> list[str]:
        return ["avro_schema"]


MESSAGE_SERIALIZER_FLAG = "MessageSerializer"

_BaseModelT = TypeVar("_BaseModelT", bound=BaseModel)
Json = Annotated[_BaseModelT, JsonSerializer, MESSAGE_SERIALIZER_FLAG]
Avro = Annotated[_BaseModelT, AvroSerializer, MESSAGE_SERIALIZER_FLAG]
