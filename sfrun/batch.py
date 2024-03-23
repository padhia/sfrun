"Batch SQL runner for Snowflake database"

import logging
from argparse import ArgumentParser, ArgumentTypeError
from enum import StrEnum
from pathlib import Path
from typing import Any

from sfconn import with_connection, with_connection_args
from snowflake.connector import SnowflakeConnection

from .runner import SqlScript
from .util import __version__, intersperse, natural

logger = logging.getLogger(__name__)


class ErrorAction(StrEnum):
    STOP = "stop"
    CONTINUE = "continue"
    SKIP_FILE = "skip-file"


def run(
    cnx: SnowflakeConnection,
    scripts: list[Path | str | None],
    on_error: ErrorAction = ErrorAction.STOP,
    **options: Any,
) -> int:
    "run each sql from each file, or stdin if no files are supplied"
    def run_script(x: SqlScript) -> bool:
        return x.run(cnx)

    runner = (SqlScript(f, stop_on_error=on_error is not ErrorAction.CONTINUE, **options) for f in scripts)
    runs = intersperse(run_script, runner)

    if on_error is ErrorAction.STOP:
        return 1 if not all(runs) else 0

    return 1 if sum(1 for r in runs if not r) > 0 else 0


@with_connection_args(__doc__)
def getargs(parser: ArgumentParser) -> None:
    "get runtime parameters"

    def file_path(v: str) -> Path | None:
        if v == "-":
            return None
        p = Path(v)
        if p.is_file():
            return p

        raise ArgumentTypeError(f"'{v}' is not an existing file")

    def a_dir(v: str) -> Path:
        p = Path(v)
        if p.is_dir():
            return p

        raise ArgumentTypeError(f"'{v}' is not an existing directory")

    parser.add_argument("input", metavar="FILE", type=file_path, nargs="*", default=[], help="SQL/Snowparkscript")
    parser.add_argument("-f", "--file", type=file_path, action="append", default=[], help="SQL/Snowparkscript")
    parser.add_argument("-t", "--table", action="append", default=[], help="table/view name")
    parser.add_argument("-q", "--query", metavar="SQL", action="append", default=[], help="SQL query")
    parser.add_argument("-p", "--pretty", action="store_true", help="print nicer column titles")
    parser.add_argument(
        "-l", "--limit", type=natural, default=500, help="limit maximum number of rows printed, 0 for no limit (default 500)"
    )
    parser.add_argument(
        "--on-error",
        choices=[x.value for x in ErrorAction],
        type=ErrorAction,
        default=ErrorAction.STOP,
        help="action to take on error: stop, continue, skip-file (default: stop)",
    )
    parser.add_argument(
        "-o", "--out-dir", type=a_dir, help="store outputs in this directory with same name as DDL but with .out extension"
    )
    parser.add_argument("--version", action="version", version=__version__)


def cli(args: list[str] | None = None) -> int:
    "cli entry-point"

    @with_connection(logger)
    def main(
        cnx: SnowflakeConnection,
        input: list[Path | None],
        file: list[Path | None],
        table: list[str],
        query: list[str],
        **kwargs: Any,
    ) -> int:
        "proxy for the main() function"
        inputs: list[Path | str | None] = input + file + [f"select * from {t}" for t in table] + query
        if not inputs:
            inputs = [None]
        return run(cnx, scripts=inputs, **kwargs)

    return main(**vars(getargs(args)))  # type: ignore
