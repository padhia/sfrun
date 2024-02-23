"""save output of SQL as CSV"""
from argparse import ArgumentParser
from typing import Any, Iterable, Optional

from yappt import tabulate


def export(rows: Iterable[tuple[Any, ...]], headers: list[str], types: list[type]) -> None:
    """export rows with given metadata"""
    tabulate(rows, headers=headers, types=[Optional[t] for t in types])  # type: ignore


def add_args(_: ArgumentParser) -> Any:
    """output args"""
