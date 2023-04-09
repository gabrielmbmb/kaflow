from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Union,
)

from pydantic import AnyUrl, BaseModel, Field

from kaflow.logger import logger

# Taken from: https://github.com/tiangolo/fastapi/blob/master/fastapi/openapi/models.py
try:
    import email_validator  # type: ignore

    assert email_validator
    from pydantic import EmailStr
except ImportError:  # pragma: no cover

    class EmailStr(str):  # type: ignore
        @classmethod
        def __get_validators__(cls) -> Iterable[Callable[..., Any]]:
            yield cls.validate

        @classmethod
        def validate(cls, v: Any) -> str:
            logger.warning(
                "`email-validator` not installed, email fields will be treated as"
                " str.\nTo install, run: `pip install email-validator`"
            )
            return str(v)


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


class ServerBinding(BaseModel):
    schemaRegistryUrl: Optional[AnyUrl] = None
    schemaRegistryVendor: Optional[str] = None
    bindingVersion: Optional[str] = None


class TopicConfiguration(BaseModel):
    cleanup_policy: Optional[str] = Field(None, alias="cleanup.policy")
    retention_ms: Optional[int] = Field(None, alias="retention.ms")
    retention_bytes: Optional[int] = Field(None, alias="retention.bytes")
    delete_retention_ms: Optional[int] = Field(None, alias="delete.retention.ms")
    max_message_bytes: Optional[int] = Field(None, alias="max.message.bytes")


class ChannelBinding(BaseModel):
    topic: Optional[str] = None
    partitions: Optional[int] = None
    replicas: Optional[int] = None
    topicConfiguration: Optional[TopicConfiguration] = None
    bindingVersion: Optional[str] = None


class OperationBinding(BaseModel):
    groupId: Optional[Schema] = None
    clientId: Optional[Schema] = None
    bindingVersion: Optional[str] = None


class MessageBinding(BaseModel):
    key: Optional[Schema] = None
    schemaIdLocation: Optional[str] = None
    schemaIdPayloadEncoding: Optional[str] = None
    schemaLookupStrategy: Optional[str] = None
    bindingVersion: Optional[str] = None


class Server(BaseModel):
    url: str
    protocol: str
    protocolVersion: Optional[str] = None
    description: Optional[str] = None
    variables: Optional[Mapping[str, Union[ServerVariable, Reference]]] = None
    security: Optional[List[Mapping[str, Any]]] = None
    bindings: Optional[Mapping[str, Union[ServerBinding, Reference]]] = None


class ExternalDocs(BaseModel):
    description: Optional[str] = None
    url: AnyUrl


class Tag(BaseModel):
    name: str
    description: Optional[str] = None
    externalDocs: Optional[ExternalDocs] = None


class BaseOperation(BaseModel):
    operationId: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[Tag]] = None
    externalDocs: Optional[ExternalDocs] = None
    bindings: Optional[Mapping[str, Union[OperationBinding, Reference]]] = None


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
    description: Optional[str] = None
    format: Optional[str] = None
    default: Optional[Any] = None
    discriminator: Optional[str] = None
    externalDocs: Optional[ExternalDocs] = None
    deprecated: Optional[bool] = None


class Parameter(BaseModel):
    description: Optional[str] = None
    schema_: Optional[Schema] = Field(None, alias="schema")
    location: Optional[str] = None


class Channel(BaseModel):
    ref: Optional[str] = Field(None, alias="$ref")
    description: Optional[str] = None
    servers: Optional[List[str]] = None
    subscribe: Optional[Operation] = None
    publish: Optional[Operation] = None
    parameters: Optional[Dict[str, Union[Parameter, Reference]]] = None
    bindings: Optional[Mapping[str, Union[ChannelBinding, Reference]]] = None


class CorrelationId(BaseModel):
    description: Optional[str] = None
    location: str


class MessageExample(BaseModel):
    headers: Optional[Mapping[str, Any]] = None
    payload: Any
    name: Optional[str] = None
    summary: Optional[str] = None


class BaseMessage(BaseModel):
    messageId: Optional[str] = None
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
    bindings: Optional[Mapping[str, Union[MessageBinding, Reference]]] = None
    examples: Optional[List[MessageExample]] = None


class MessageTrait(BaseMessage):
    pass


class Message(BaseMessage):
    traits: Optional[List[Union[MessageTrait, Reference]]] = None


class OAuthFlow(BaseModel):
    authorizationUrl: Optional[AnyUrl] = None
    tokenUrl: Optional[AnyUrl] = None
    refreshUrl: Optional[AnyUrl] = None
    scopes: Mapping[str, str]


class OAuthFlows(BaseModel):
    implicit: Optional[OAuthFlow] = None
    password: Optional[OAuthFlow] = None
    clientCredentials: Optional[OAuthFlow] = None
    authorizationCode: Optional[OAuthFlow] = None


class SecurityScheme(BaseModel):
    type: Literal[
        "userPassword",
        "apiKey",
        "X509",
        "symmetrictEncryption",
        "asymmetricEncryption",
        "httpApiKey",
        "http",
        "oauth2",
        "openIdConnect",
        "plain",
        "scramSha256",
        "scramSha512",
        "gssapi",
    ]
    description: Optional[str] = None
    name: Optional[str] = None
    in_: Optional[Literal["user", "password", "query", "header", "cookie"]] = None
    scheme: Optional[str] = None
    bearerFormat: Optional[str] = None
    flows: Optional[OAuthFlows] = None
    openIdConnectUrl: Optional[AnyUrl] = None


class Components(BaseModel):
    schemas: Optional[Mapping[str, Union[Schema, Reference]]] = None
    servers: Optional[Mapping[str, Union[Server, Reference]]] = None
    serverVariables: Optional[Mapping[str, Union[ServerVariable, Reference]]] = None
    channels: Optional[Mapping[str, Union[Channel, Reference]]] = None
    messages: Optional[Mapping[str, Union[Message, Reference]]] = None
    securitySchemes: Optional[Mapping[str, Union[SecurityScheme, Reference]]] = None
    parameters: Optional[Mapping[str, Union[Parameter, Reference]]] = None
    correlationIds: Optional[Mapping[str, Union[CorrelationId, Reference]]] = None
    operationTraits: Optional[Mapping[str, Union[OperationTrait, Reference]]] = None
    messageTraits: Optional[Mapping[str, Union[MessageTrait, Reference]]] = None
    serverBindings: Optional[Mapping[str, Union[ServerBinding, Reference]]] = None
    channelBindings: Optional[Mapping[str, Union[ChannelBinding, Reference]]] = None
    operationBindings: Optional[Mapping[str, Union[OperationBinding, Reference]]] = None
    messageBindings: Optional[Mapping[str, Union[MessageBinding, Reference]]] = None


class AsyncAPI(BaseModel):
    """A model containing all the information required to generate an AsyncAPI spec.

    https://www.asyncapi.com/docs/reference/specification/v2.6.0
    """

    asyncapi: str
    id: Optional[str] = None
    info: Info
    servers: Optional[Mapping[str, Server]] = None
    defaultContentType: Optional[str] = None
    channels: Mapping[str, Channel] = {}
    components: Optional[Components] = None
    tags: Optional[List[Tag]] = None
    externalDocs: Optional[ExternalDocs] = None
