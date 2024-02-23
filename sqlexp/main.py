"Snowflake SQL pretty printer"
from __future__ import annotations

import sys
from argparse import ArgumentParser, ArgumentTypeError
from itertools import islice
from pathlib import Path
from string import whitespace
from typing import Any, Callable, Iterable, Optional

from sfconn import Connection, Cursor, DatabaseError, pytype, with_connection, with_connection_args
from snowflake.connector.constants import FIELD_TYPES

from . import csv, fmt, md, xls

RowType = tuple[Any, ...]
ExporterFn = Callable[[Iterable[RowType], list[str], list[type]], None]

__version__ = "0.1.0"


@with_connection
def main(cnx: Connection, sql: Optional[str], **kwargs: Any) -> None:
    "script entry-point"
    if sql is None:
        sql = sql_from_file("-")

    with cnx.cursor() as csr:
        sql_export(csr, sql, **kwargs)


def sql_export(
    csr: Cursor,
    sql: str,
    limit: int = 100,
    hard_limit: Optional[int] = None,
    pretty_headers: bool = True,
    describe: bool = False,
    export: ExporterFn = fmt.export,
    **kwargs: Any,
) -> None:
    if hard_limit is not None:
        sql += f"\nLIMIT {hard_limit}"

    try:
        meta = csr.describe(sql)  # type: ignore
    except DatabaseError as ex:
        raise SystemExit(str(ex))

    if describe:
        headers = ["Name", "Type", "Size", "Prec", "Scale", "Nulls?"]
        types = [str, str, int, int, int, bool]
        data: Iterable[RowType] = (
            (m.name, FIELD_TYPES[m.type_code].name, m.internal_size, m.precision, m.scale, m.is_nullable) for m in meta
        )
    else:
        headers = [m.name.replace("_", " ").title() for m in meta] if pretty_headers else [m.name for m in meta]
        types = [pytype(d) for d in meta]
        data = csr.run_query(lambda *r: r, sql)
        if hard_limit is None and limit > 0:
            data = islice(data, limit)

    export(data, headers, types, **kwargs)


def raw_export(rows: Iterable[RowType], *_: Any) -> None:
    """export raw data"""
    for r in rows:
        print("\t".join(r))


def sql_from_file(v: str) -> str:
    if v == "-":
        sql = sys.stdin.read()
    else:
        try:
            sql = Path(v).read_text()
        except FileNotFoundError:
            raise ArgumentTypeError(f"file '{v}' could not be read")
    return sql.rstrip(whitespace + ";")


@with_connection_args(__doc__)
def getargs(parser: ArgumentParser) -> Any:
    """parse user supplied args"""

    def natural(v: str) -> int:
        try:
            if (n := int(v)) >= 0:
                return n
        except ValueError:
            pass

        raise ArgumentTypeError("must be a positive whole number")

    x = parser.add_mutually_exclusive_group()
    x.add_argument("-f", "--inpfile", dest="sql", type=sql_from_file, help="input file name (- for stdin)")
    x.add_argument("-t", "--table", dest="sql", type=lambda t: "SELECT * FROM " + t, help="input is a table or view name")

    x = parser.add_mutually_exclusive_group()
    x.add_argument("-l", "--limit", metavar="NUM", default=100, type=natural, help="fetch only N rows (default 100)")
    x.add_argument("-L", "--hard-limit", metavar="NUM", type=natural, help="add a LIMIT N clause to the SQL")

    g = parser.add_argument_group("output options")
    g.add_argument("-P", "--pretty-headers", action="store_true", help="print pretty headers")

    parser.add_argument("--describe", "--desc", action="store_true", help="describe the query metadata instead of running it")

    s = parser.add_subparsers(help="formatting option")
    parser.set_defaults(export=fmt.export)

    p = s.add_parser("fmt", help="Output in simple tabular format")
    p.set_defaults(export=fmt.export)
    fmt.add_args(p)

    p = s.add_parser("csv", help="Output in CSV format")
    p.set_defaults(export=csv.export)
    csv.add_args(p)

    p = s.add_parser("md", help="Output in Markdown format")
    p.set_defaults(export=md.export)
    md.add_args(p)

    p = s.add_parser("raw", help="Output in raw format (no headers, tab separated columns)")
    p.set_defaults(export=raw_export)

    p = s.add_parser("xls", help="Output in MS Excel format")
    p.set_defaults(export=xls.export)
    xls.add_args(p)

    parser.add_argument("--version", action="version", version=__version__)


def cli() -> None:
    "cli entry-point"
    main(**vars(getargs()))
