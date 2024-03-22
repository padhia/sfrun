"Snowflake SQL pretty printer"

import datetime as dt
import importlib.util as iu
import sys
from argparse import ArgumentParser, ArgumentTypeError
from decimal import Decimal
from itertools import islice
from pathlib import Path
from string import whitespace
from typing import Any, Callable, Iterable, NamedTuple, TextIO, TypeAlias

import snowflake.snowpark.types as T
from sfconn import Connection, Cursor, DatabaseError, pytype, with_connection, with_connection_args, with_session
from snowflake.connector.constants import FIELD_TYPES
from snowflake.snowpark import DataFrame, Session

from . import csv, fmt, md, xls

RowType: TypeAlias = tuple[Any, ...]
ExporterFn: TypeAlias = Callable[[Iterable[RowType], list[str], list[type], TextIO], None]
SnowparkFn: TypeAlias = Callable[[Session], DataFrame]

__version__ = "0.1.0"


class Limit(NamedTuple):
    rows: int
    hard: bool


class Exporter(NamedTuple):
    fn: ExporterFn
    output: Path | None

    def export(self, data: Iterable[RowType], headers: list[str], types: list[type]):
        if self.output is None:
            self.fn(data, headers, types, sys.stdout)
        else:
            with self.output.open("w") as f:
                self.fn(data, headers, types, f)


def main(sql: str | None, fn: SnowparkFn | None = None, **kwargs: Any) -> None:
    "script entry-point"

    @with_connection
    def run_sql(cnx: Connection, sql: str | None, **kwargs: Any) -> None:
        if sql is None:
            sql = sql_from_file("-")

        with cnx.cursor() as csr:
            sql_export(csr, sql, **kwargs)

    @with_session
    def run_df(session: Session, **kwargs: Any) -> None:
        df_export(session, **kwargs)

    return run_sql(sql=sql, **kwargs) if fn is None else run_df(fn=fn, **kwargs)


def to_pytype(t: T.DataType) -> type:
    types = {
        T.DateType: dt.date,
        T.BooleanType: bool,
        T.DecimalType: Decimal,
        T.DoubleType: float,
    }
    return next((py_t for sp_t, py_t in types.items() if isinstance(t, sp_t)), str)


def pretty_name(name: str) -> str:
    return name.replace("_", " ").title()


def df_export(
    session: Session,
    fn: SnowparkFn,
    limit: int = 100,
    hard_limit: int | None = None,
    describe: bool = False,
    export: Exporter | Path = Exporter(fmt.export, None),
    pretty_headers: bool = False,
) -> None:
    df = fn(session).limit(limit)
    schema = df.schema
    headers = [pretty_name(f.name) for f in schema.fields] if pretty_headers else [f.name for f in schema.fields]
    types = [to_pytype(f.datatype) for f in schema.fields]
    data = df.to_local_iterator()

    export.export(data, headers, types) if isinstance(export, Exporter) else xls.export(data, headers, types, export)


def sql_export(
    csr: Cursor,
    sql: str,
    limit: Limit | None = None,
    describe: bool = False,
    export: Exporter | Path = Exporter(fmt.export, None),
    pretty_headers: bool = False,
) -> None:
    if limit and limit.hard:
        sql += f"\nLIMIT {limit.rows}"

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
        headers = [pretty_name(m.name) for m in meta] if pretty_headers else [m.name for m in meta]
        types = [pytype(d) for d in meta]
        data = csr.run_query(lambda *r: r, sql)
        if limit and not limit.hard:
            data = islice(data, limit.rows)

    export.export(data, headers, types) if isinstance(export, Exporter) else xls.export(data, headers, types, export)


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

    def limit(hard: bool) -> Callable[[str], Limit]:
        def natural(v: str) -> Limit:
            try:
                if (n := int(v)) >= 0:
                    return Limit(n, hard)
            except ValueError:
                pass
            raise ArgumentTypeError("must be a positive whole number")

        return natural

    def exporter(fn: ExporterFn) -> Callable[[str], Exporter]:
        def wrapped(v: str) -> Exporter:
            return Exporter(fn, Path(v))

        return wrapped

    def py_file(v: str) -> SnowparkFn:
        script = Path(v)

        if not script.is_file() or script.suffix != ".py":
            raise ArgumentTypeError(f"'{v}' is not a existing Python script")
        if (spec := iu.spec_from_file_location("sprun", script)) is None:
            raise ArgumentTypeError(f"Error loading {script}")
        if (loader := spec.loader) is None:
            raise ArgumentTypeError(f"Error loading {script}")

        module = iu.module_from_spec(spec)
        loader.exec_module(module)

        return module.main

    x = parser.add_mutually_exclusive_group()
    x.add_argument("-f", "--file", dest="sql", metavar="FILE", type=sql_from_file, help="SQL file name (- for stdin)")
    x.add_argument("-t", "--table", dest="sql", metavar="TABLE", type=lambda t: "SELECT * FROM " + t, help="Table (or view) name")
    x.add_argument("-p", "--python", dest="fn", metavar="FILE", type=py_file, help="Python script returning a Snowpark DataFrame")

    x = parser.add_mutually_exclusive_group()
    x.add_argument("-l", "--limit", metavar="NUM", type=limit(False), help="fetch only N rows (default 100)")
    x.add_argument("-L", "--hard-limit", dest="limit", metavar="NUM", type=limit(True), help="add a LIMIT N clause to the SQL")

    parser.add_argument("--describe", "--desc", action="store_true", help="describe the query metadata instead of running it")

    g = parser.add_argument_group("output options")
    g.add_argument("-P", "--pretty-headers", action="store_true", help="print pretty headers")

    x = g.add_mutually_exclusive_group()

    def add_export(opt: str, fn: ExporterFn, name: str):
        x.add_argument(
            f"--{opt}",
            dest="export",
            metavar="FILE",
            type=exporter(fn),
            default=Exporter(fmt.export, None),
            nargs="?",
            const=Exporter(fn, None),
            help=f"write output in {name} format",
        )

    add_export("fmt", fmt.export, "tabular")
    add_export("csv", csv.csv_export, "CSV")
    add_export("tsv", csv.csv_export, "TSV")
    add_export("md", md.export, "markdown")
    add_export("raw", raw_export, "raw (no headers, tab delimited)")
    x.add_argument("--xls", dest="export", metavar="FILE", type=Path, help="write output in MS Excel format")

    parser.add_argument("--version", action="version", version=__version__)


def cli() -> None:
    "cli entry-point"
    main(**vars(getargs()))  # type: ignore
