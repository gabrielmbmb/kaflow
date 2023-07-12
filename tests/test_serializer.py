from __future__ import annotations

from kaflow.serializers import Serializer


def test_serializer_required_extra_annotation_keys() -> None:
    assert Serializer.required_extra_annotations_keys() == []
