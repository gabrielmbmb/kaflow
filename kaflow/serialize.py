import json
from typing import Annotated, Any, Protocol, TypeVar

from pydantic import BaseModel


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
    pass


_BaseModelT = TypeVar("_BaseModelT", bound=BaseModel)
Json = Annotated[_BaseModelT, JsonSerializer]
Avro = Annotated[_BaseModelT, AvroSerializer]
