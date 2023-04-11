from kaflow.applications import Kaflow
from kaflow.dependencies import Depends
from kaflow.message import Message
from kaflow.parameters import (
    FromHeader,
    FromKey,
    FromValue,
    MessageOffset,
    MessagePartition,
    MessageTimestamp,
)
from kaflow.serializers import Json, String, has_fastavro, has_protobuf

__all__ = [
    "Kaflow",
    "Depends",
    "Message",
    "FromHeader",
    "FromKey",
    "FromValue",
    "MessageOffset",
    "MessagePartition",
    "MessageTimestamp",
    "Json",
    "String",
]

if has_fastavro:
    from kaflow.serializers import Avro  # noqa: F401

    __all__.append("Avro")

if has_protobuf:
    from kaflow.serializers import Protobuf  # noqa: F401

    __all__.append("Protobuf")

__version__ = "0.1.4"
