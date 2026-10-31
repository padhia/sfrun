"Utility types and functions"

import os
from argparse import ArgumentTypeError
from collections.abc import Callable, Iterable
from enum import Enum, auto
from logging import getLogger
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateSyntaxError, UndefinedError
from snowflake.snowpark import DataFrame, Session

type Data = Iterable[tuple[Any, ...]]
type SnowparkFn = Callable[[Session], DataFrame | str]


SOFT_LIMIT = 100

__version__ = "0.2.0"
logger = getLogger(__name__)


class Command(Enum):
    EXPORT = auto()
    SHOW_SQL = auto()
    SHOW_SCHEMA = auto()


def intersperse[T, U](fn: Callable[[T], U], xs: Iterable[T]) -> Iterable[U]:
    "add a new line between items and return and Iterable over input mapped by fn"
    is_first = True
    for x in xs:
        if is_first:
            is_first = False
        else:
            print()
        yield fn(x)


def natural(v: str) -> int:
    try:
        if (n := int(v)) >= 0:
            return n
    except ValueError:
        pass
    raise ArgumentTypeError("must be a positive whole number")


def prettify(headers: Iterable[str]) -> list[str]:
    return [x.replace("_", " ").title() for x in headers]


class _Env(dict[str, str]):
    "environment variables exposed to SQL templates as ctx.env"


class _SqlUndefined(StrictUndefined):
    @property
    def _undefined_message(self) -> str:
        if isinstance(self._undefined_obj, _Env):
            return f"environment variable '{self._undefined_name}' is not set"
        return super()._undefined_message


def render_sql(path: Path) -> str:
    "read SQL text from a file and render it as a Jinja2 template; ctx.env.X expands to environment variable X"
    env = Environment(loader=FileSystemLoader(path.parent), undefined=_SqlUndefined, keep_trailing_newline=True)
    try:
        return env.get_template(path.name).render(ctx={"env": _Env(os.environ)})
    except UndefinedError as err:
        raise SystemExit(f"error: {path}: {err}") from err
    except TemplateSyntaxError as err:
        raise SystemExit(f"error: {path}:{err.lineno}: {err.message}") from err


def take(data: Data, limit: int | None = None) -> Data:
    if limit is None:
        limit = SOFT_LIMIT

    for e, row in enumerate(data, start=1):
        if e > limit:
            logger.warning(f"data truncated after {limit} rows")
            return
        yield row
