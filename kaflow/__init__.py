from kaflow.applications import Kaflow
from kaflow.serializers import Json, has_fastavro, has_protobuf

__all__ = ["Kaflow", "Json"]

if has_fastavro:
    from kaflow.serializers import Avro  # noqa: F401

    __all__.append("Avro")

if has_protobuf:
    from kaflow.serializers import Protobuf  # noqa: F401

    __all__.append("Protobuf")

__version__ = "0.0.1"
