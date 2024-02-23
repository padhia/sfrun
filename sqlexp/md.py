"save SQL result-set in markdown table format"
import sys
from argparse import ArgumentParser, FileType
from typing import Any, Iterable, TextIO


def export(rows: Iterable[tuple[Any, ...]], headers: list[str], types: list[type], output: TextIO) -> None:
    """export rows with given metadata"""

    def sep(t: type) -> str:
        return "--:" if issubclass(t, (float, int)) else "---"

    def emit(xs: Iterable[str]) -> None:
        print("|" + "|".join(xs) + "|", file=output)

    emit(headers)
    emit(sep(t) for t in types)
    for r in rows:
        emit(str(c) for c in r)


def add_args(parser: ArgumentParser) -> Any:
    """output args"""
    parser.add_argument("-o", "--output", metavar="FILE", type=FileType("w"), default=sys.stdout, help="output file name")
