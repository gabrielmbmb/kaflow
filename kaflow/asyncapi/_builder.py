from __future__ import annotations

from typing import TYPE_CHECKING, Any

from kaflow.asyncapi import models

if TYPE_CHECKING:
    from kaflow.topic import TopicProcessor
    from kaflow.typing import ProducerFunc


def build_asyncapi(
    *,
    asyncapi_version: str,
    title: str,
    version: str,
    description: str | None = None,
    terms_of_service: str | None = None,
    contact: dict[str, Any] | None = None,
    license_info: dict[str, Any] | None = None,
    topic_processors: dict[str, TopicProcessor],
    producers: dict[str, list[ProducerFunc]],
) -> models.AsyncAPI:
    asyncapi_info = {"title": title, "version": version}
    if description:
        asyncapi_info["description"] = description
    if terms_of_service:
        asyncapi_info["termsOfService"] = terms_of_service
    if contact:
        asyncapi_info["contact"] = contact
    if license_info:
        asyncapi_info["license"] = license_info
    output = {"asyncapi": asyncapi_version, "info": asyncapi_info}
    channels: dict[str, dict[str, Any]] = {}
    if channels:
        output["channels"] = channels
    return models.AsyncAPI(**output)
