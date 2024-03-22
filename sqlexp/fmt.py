"""save output of SQL as CSV"""

from typing import Any, Iterable, Optional, TextIO

from yappt import tabulate


def export(rows: Iterable[tuple[Any, ...]], headers: list[str], types: list[type], file: TextIO) -> None:
    """export rows with given metadata"""
    tabulate(rows, headers=headers, types=[Optional[t] for t in types], file=file)  # type: ignore
