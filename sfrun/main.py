"Snwoflake SQL query runner; print output in different formats"

import sys
from argparse import ArgumentParser, ArgumentTypeError
from pathlib import Path
from typing import Any

from sfconn import with_connection_args

from .df import main_py
from .formats import Format
from .sql import main_sql
from .util import SOFT_LIMIT, Command, __version__, natural


def main(input: list[Path], file: list[Path], table: list[str], query: list[str], fn: str, **kwargs: Any) -> None:
    "script entry-point"
    sqls = [f.read_text() for f in (input + file) if f.suffix != ".py"] + [f"select * from {t}" for t in table] + query
    pys = [f for f in (input + file) if f.suffix == ".py"]
    if not sqls and not pys:
        sqls = [sys.stdin.read()]

    if sqls:
        main_sql(sqls=sqls, **kwargs)

    if pys:
        main_py(pys, fn, **kwargs)


@with_connection_args(__doc__)
def getargs(parser: ArgumentParser) -> Any:
    "parse user supplied args"

    def existing_file(v: str) -> Path:
        if (path := Path(v)).is_file():
            return path
        raise ArgumentTypeError(f"'{v}' is not an existing file")

    g = parser.add_argument_group("Input options")
    g.add_argument("input", type=existing_file, nargs="*", default=[], help="SQL/Snowpark script")
    g.add_argument("-f", "--file", type=existing_file, action="append", default=[], help="SQL/Snowpark script")
    g.add_argument("-t", "--table", metavar="NAME", action="append", default=[], help="table/view name")
    g.add_argument("-q", "--query", metavar="SQL", action="append", default=[], help="SQL query")

    parser.add_argument("-l", "--limit", metavar="NUM", type=natural, help=f"fetch only N rows (default {SOFT_LIMIT})")
    parser.add_argument("-P", "--pretty-headers", action="store_true", help="print, when applicable, human readable headers")

    x = parser.add_mutually_exclusive_group()
    x.set_defaults(cmd=Command.EXPORT)
    x.add_argument(
        "--show-sql",
        action="store_const",
        const=Command.SHOW_SQL,
        dest="cmd",
        help="show generated SQL instead of running it",
    )
    x.add_argument(
        "--describe",
        "--desc",
        "--show-schema",
        action="store_const",
        const=Command.SHOW_SCHEMA,
        dest="cmd",
        help="describe the query metadata instead of running it",
    )

    parser.add_argument("--fn", default="main", help="for Snowpark scripts, Python function name (default: main)")

    Format.add_args(parser)

    parser.add_argument("--version", action="version", version=__version__)


def cli() -> None:
    "cli entry-point"
    main(**vars(getargs()))  # type: ignore
