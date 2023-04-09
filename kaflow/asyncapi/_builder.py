from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from pydantic.schema import get_model_name_map, model_process_schema
from typing_extensions import TypeGuard

from kaflow.asyncapi import models

if TYPE_CHECKING:
    from kaflow._consumer import TopicConsumerFunc
    from kaflow.applications import ProducerFunc


def not_bytes_and_base_model(cls: type) -> TypeGuard[type[BaseModel]]:
    return cls is not None and cls is not bytes and issubclass(cls, BaseModel)


def get_flat_models(
    consumers: dict[str, TopicConsumerFunc] | None = None
) -> set[type[BaseModel] | type[Enum]]:
    models: set[type[BaseModel] | type[Enum]] = set()
    if consumers:
        for consumer in consumers.values():
            if not_bytes_and_base_model(consumer.value_param_type):
                models.add(consumer.value_param_type)
            if consumer.key_param_type:
                if not_bytes_and_base_model(consumer.key_param_type):
                    models.add(consumer.key_param_type)
            if consumer.headers_type_deserializers:
                for header in consumer.headers_type_deserializers:
                    header_type = consumer.headers_type_deserializers[header][0]
                    if not_bytes_and_base_model(header_type):
                        models.add(header_type)
    return models


def get_model_definitions(
    flat_models: set[type[BaseModel] | type[Enum]],
    model_name_map: dict[type[BaseModel] | type[Enum], str],
) -> dict[str, Any]:
    definitions: dict[str, dict[str, Any]] = {}
    for model in flat_models:
        model_schema, model_definitions, model_nested_models = model_process_schema(
            model, model_name_map=model_name_map
        )
        definitions.update(model_definitions)
        model_name = model_name_map[model]
        definitions[model_name] = model_schema
    return definitions


def build_asyncapi(
    asyncapi_version: str,
    title: str,
    version: str,
    description: str | None = None,
    terms_of_service: str | None = None,
    contact: dict[str, Any] | None = None,
    license_info: dict[str, Any] | None = None,
    consumers: dict[str, TopicConsumerFunc] | None = None,
    producers: dict[str, list[ProducerFunc]] | None = None,
) -> models.AsyncAPI:
    asyncapi_info: dict[str, Any] = {"title": title, "version": version}
    if description:
        asyncapi_info["description"] = description
    if terms_of_service:
        asyncapi_info["termsOfService"] = terms_of_service
    if contact:
        asyncapi_info["contact"] = contact
    if license_info:
        asyncapi_info["license"] = license_info
    components: dict[str, dict[str, Any]] = {}
    flat_models = get_flat_models(consumers)
    model_name_map = get_model_name_map(flat_models)
    definitions = get_model_definitions(flat_models, model_name_map)
    if definitions:
        components["schemas"] = definitions
    output = {
        "asyncapi": asyncapi_version,
        "info": asyncapi_info,
        "components": components,
    }
    return models.AsyncAPI(**output)
