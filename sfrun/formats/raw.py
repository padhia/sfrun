from typing import TextIO

from ..util import Data, textio


@textio
def export(rows: Data, headers: list[str], types: list[type], file: TextIO) -> None:
    """export data in raw format (textual, tab delimited)"""
    for r in rows:
        print("\t".join(str(c) for c in r), file=file)
