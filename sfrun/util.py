"Utility types and functions"

import sys
from argparse import ArgumentTypeError
from enum import Enum, StrEnum, auto
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, Iterable, TextIO, TypeAlias, TypeVar

from snowflake.snowpark import DataFrame, Session

Data: TypeAlias = Iterable[tuple[Any, ...]]
ExportFn = Callable[[Data, list[str], list[type]], None]
SnowparkFn: TypeAlias = Callable[[Session], DataFrame | str]
T = TypeVar("T")
U = TypeVar("U")

SOFT_LIMIT = 100

__version__ = "0.1.0"
logger = getLogger(__name__)


class Command(Enum):
    EXPORT = auto()
    SHOW_SQL = auto()
    SHOW_SCHEMA = auto()


class ErrorAction(StrEnum):
    STOP = "stop"
    CONTINUE = "continue"
    SKIP_FILE = "skip-file"


def intersperse(fn: Callable[[T], U], xs: Iterable[T]) -> Iterable[U]:
    "add a new line between items and return and Iterable over input mapped by fn"
    is_first = True
    for x in xs:
        if is_first:
            is_first = False
        else:
            print()
        yield fn(x)


def textio(
    fn: Callable[[Data, list[str], list[type], TextIO], None],
) -> Callable[[Data, list[str], list[type], Path | None], None]:
    def wrapped(rows: Data, headers: list[str], types: list[type], output: Path | None):
        if output is None:
            fn(rows, headers, types, sys.stdout)
        else:
            with output.open("w") as f:
                fn(rows, headers, types, f)

    return wrapped


def natural(v: str) -> int:
    try:
        if (n := int(v)) >= 0:
            return n
    except ValueError:
        pass
    raise ArgumentTypeError("must be a positive whole number")


def prettify(headers: Iterable[str]) -> list[str]:
    return [x.replace("_", " ").title() for x in headers]


def take(data: Data, limit: int | None = None) -> Data:
    if limit is None:
        limit = SOFT_LIMIT

    for e, row in enumerate(data, start=1):
        if e > limit:
            logger.warning(f"data truncated after {limit} rows")
            return
        yield row
