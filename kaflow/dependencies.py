from __future__ import annotations

from typing import Any, Literal

from di.dependent import Marker

Scope = Literal["app", "consumer"]
Scopes = ("app", "consumer")


def Depends(
    call: Any = None,
    *,
    use_cache: bool = True,
    wire: bool = True,
    scope: Scope | None = None,
) -> Marker:
    return Marker(
        call=call,
        use_cache=use_cache,
        wire=wire,
        scope=scope,
    )
