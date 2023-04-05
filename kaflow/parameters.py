from typing import TypeVar

from typing_extensions import Annotated

T = TypeVar("T")

FROM_VALUE_FLAG = "from_value"
FromValue = Annotated[T, FROM_VALUE_FLAG]

FROM_KEY_FLAG = "from_key"
FromKey = Annotated[T, FROM_KEY_FLAG]

FROM_HEADER_FLAG = "from_header"
FromHeader = Annotated[T, FROM_HEADER_FLAG]
