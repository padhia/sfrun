"""save output of SQL as CSV"""

from typing import Optional, TextIO

from yappt import tabulate

from ..util import Data, textio


@textio
def export(rows: Data, headers: list[str], types: list[type], file: TextIO) -> None:
    "export formatted data"
    tabulate(rows, headers=headers, types=[Optional[t] for t in types], file=file)  # type: ignore
