from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import AnyUrl, BaseModel, EmailStr, Field


class Contact(BaseModel):
    name: Optional[str] = None
    url: Optional[AnyUrl] = None
    email: Optional[EmailStr] = None


class License(BaseModel):
    name: str
    url: Optional[AnyUrl] = None


class Info(BaseModel):
    title: str
    version: str
    description: Optional[str] = None
    termsOfService: Optional[str] = None
    contact: Optional[Contact] = None
    license: Optional[License] = None


class ServerVariable(BaseModel):
    enum: Optional[List[str]] = None
    default: Optional[str] = None
    description: Optional[str] = None
    examples: Optional[List[str]] = None


class Binding(BaseModel):
    pass


class Server(BaseModel):
    url: AnyUrl
    protocol: str
    protocolVersion: Optional[str] = None
    description: Optional[str] = None
    variables: Optional[Dict[str, ServerVariable]] = None
    security: Optional[Dict[str, List[str]]] = None
    bindings: Optional[Dict[str, Binding]] = None


class ExternalDocs(BaseModel):
    description: Optional[str] = None
    url: AnyUrl


class Tag(BaseModel):
    name: str
    description: Optional[str] = None
    externalDocs: Optional[ExternalDocs] = None


class Trait(BaseModel):
    operationId: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[Tag]] = None
    externalDocs: Optional[ExternalDocs] = None
    bindings: Optional[Dict[str, Binding]] = None


class BaseOperation(BaseModel):
    operationId: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[Tag]] = None
    externalDocs: Optional[ExternalDocs] = None
    bindings: Optional[Dict[str, Binding]] = None


class OperationTrait(BaseOperation):
    pass


class Operation(BaseOperation):
    traits: Optional[List[OperationTrait]] = None


class Reference(BaseModel):
    ref: str = Field(..., alias="$ref")


class Schema(BaseModel):
    title: Optional[str] = None
    type: Optional[str] = None
    required: Optional[List[str]] = None
    multipleOf: Optional[float] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[float] = None
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[float] = None
    maxLength: Optional[int] = None
    minLength: Optional[int] = None
    pattern: Optional[str] = None
    maxItems: Optional[int] = None
    minItems: Optional[int] = None
    uniqueItems: Optional[bool] = None
    maxProperties: Optional[int] = None
    minProperties: Optional[int] = None
    enum: Optional[List[Any]] = None
    const: Optional[str] = None
    examples: Optional[List[str]] = None
    readOnly: Optional[bool] = None
    writeOnly: Optional[bool] = None
    properties: Optional[Dict[str, Schema]] = None
    patternProperties: Optional[Dict[str, Schema]] = None
    additionalProperties: Optional[Schema] = None
    additionalItems: Optional[Schema] = None
    items: Optional[Union[Schema, List[Schema]]] = None
    propertyNames: Optional[Schema] = None
    contains: Optional[Schema] = None
    allOf: Optional[List[Schema]] = None
    oneOf: Optional[List[Schema]] = None
    anyOf: Optional[List[Schema]] = None
    not_: Optional[Schema] = Field(None, alias="not")
    discriminator: Optional[str] = None
    externalDocs: Optional[ExternalDocs] = None
    deprecated: Optional[bool] = None


class Parameter(BaseModel):
    description: Optional[str] = None
    schema: Optional[Schema] = None
    location: Optional[str] = None


class Channel(BaseModel):
    ref: Optional[str] = Field(None, alias="$ref")
    description: Optional[str] = None
    subscribe: Optional[Operation] = None
    publish: Optional[Operation] = None
    parameters: Optional[Dict[str, Union[Parameter, Reference]]] = None
    bindings: Optional[Dict[str, Binding]] = None


class CorrelationId(BaseModel):
    description: Optional[str] = None
    location: str


class BaseMessage(BaseModel):
    headers: Optional[Union[Schema, Reference]] = None
    payload: Any
    correlationId: Optional[Union[CorrelationId, Reference]] = None
    schemaFormat: Optional[str] = None
    contentType: Optional[str] = None
    name: Optional[str] = None
    title: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[Tag]] = None
    externalDocs: Optional[ExternalDocs] = None
    bindings: Optional[Dict[str, Binding]] = None
    examples: Optional[List[Any]] = None


class MessageTrait(BaseMessage):
    pass


class Message(BaseMessage):
    traits: Optional[List[MessageTrait]] = None


class SecurityScheme(BaseModel):
    pass


class Component(BaseModel):
    schemas: Optional[Dict[str, Schema]] = None
    messages: Optional[Dict[str, Union[Message, Reference]]] = None
    securitySchemes: Optional[Dict[str, Union[SecurityScheme, Reference]]] = None
    parameters: Optional[Dict[str, Union[Parameter, Reference]]] = None
    correlationIds: Optional[Dict[str, CorrelationId]] = None
    operationTraits: Optional[Dict[str, OperationTrait]] = None
    messageTraits: Optional[Dict[str, MessageTrait]] = None
    serverBindings: Optional[Dict[str, Binding]] = None
    channelBindings: Optional[Dict[str, Binding]] = None
    operationBindings: Optional[Dict[str, Binding]] = None
    messageBindings: Optional[Dict[str, Binding]] = None


class AsyncAPI(BaseModel):
    """A model containing all the information required to generate an AsyncAPI spec.

    https://www.asyncapi.com/docs/reference/specification/v2.0.0
    """

    asyncapi: str
    id: Optional[str] = None
    info: Info
    servers: Optional[Dict[str, Server]] = None
    channels: Dict[str, Channel] = {}
    components: Optional[List[Component]] = None
    tag: List[Tag]
    externalDocs: ExternalDocs
