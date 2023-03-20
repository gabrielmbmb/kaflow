import sys

if sys.version_info < (3, 9):
    from typing_extensions import Annotated as Annotated
else:
    from typing import Annotated as Annotated

if sys.version_info < (3, 8):
    from typing_extensions import Literal as Literal
    from typing_extensions import Protocol as Protocol
else:
    from typing import Literal as Literal
    from typing import Protocol as Protocol
