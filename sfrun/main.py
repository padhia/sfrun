"Snwoflake SQL query runner; print output in different formats"

import sys
from argparse import ArgumentParser, ArgumentTypeError
from pathlib import Path
from typing import Any

from sfconn import with_connection_args

from .df import main_py
from .formats import Exporter
from .sql import main_sql
from .util import SOFT_LIMIT, Command, __version__, natural, render_sql


def main(
    input: list[Path],
    file: list[Path],
    table: list[str],
    query: list[str],
    fn: str,
    format: str | None = None,
    output: Path | None = None,
    **kwargs: Any,
) -> None:
    "script entry-point"
    if format is not None:
        exporter = Exporter.by_name(format)
    else:
        exporter = Exporter.from_path(output) if output is not None else Exporter.default()
    if exporter.requires_file and output is None:
        raise SystemExit(f"error: --output (-o) is required when --format={exporter.name}")
    kwargs.update(exporter=exporter, output=output)

    sqls = [render_sql(f) for f in (input + file) if f.suffix != ".py"] + [f"select * from {t}" for t in table] + query
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

    def output_file(v: str) -> Path:
        path = Path(v)
        if path.exists() and not path.is_file():
            raise ArgumentTypeError(f"Output file '{v}' is not a file")
        if not (p := path.parent).exists():
            raise ArgumentTypeError(f"Cannot create output file '{v}', the parent directory '{p}' does not exist")
        return path

    formats = Exporter.registry.values()
    file_only = ", ".join(f"--format={c.name}" for c in formats if c.requires_file)

    g = parser.add_argument_group("output formatting options")
    g.add_argument(
        "--format",
        choices=list(Exporter.registry),
        help="output format: "
        + ", ".join(f"{c.name} ({c.help})" for c in formats)
        + f" (default: guessed from --output file extension, else {Exporter.default().name})",
    )
    g.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        type=output_file,
        help=f"write output to FILE instead of stdout (required for {file_only})",
    )

    parser.add_argument("--version", action="version", version=__version__)


def cli() -> None:
    "cli entry-point"
    main(**vars(getargs()))  # type: ignore
