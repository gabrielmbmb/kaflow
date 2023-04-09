from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kaflow.asyncapi.models import AsyncAPI


def get_asyncapi_html(
    title: str,
    asyncapi_schema: AsyncAPI,
    asyncapi_react_component_js_url: str = "https://unpkg.com/@asyncapi/web-component@1.0.0-next.47/lib/asyncapi-web-component.js",
    asyncapi_react_component_css_url: str = "https://unpkg.com/@asyncapi/react-component@1.0.0-next.12/styles/default.min.css",
) -> str:
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <link
            rel="stylesheet"
            href="{asyncapi_react_component_css_url}"
        />
        <title>{title}</title>
    </head>
    <body>
        <script
            src="{asyncapi_react_component_js_url}"
            defer
        ></script>
        <asyncapi-component
            schema='{asyncapi_schema.json(by_alias=True, exclude_none=True)}'
            cssImportPath="{asyncapi_react_component_css_url}"
        ></asyncapi-component>
    </body>
    </html>
    """
    return html
