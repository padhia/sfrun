"Snwoflake SQL query runner; print output in different formats"

from logging import getLogger
from pathlib import Path
from string import whitespace
from typing import Any, cast

from sfconn import pytype, with_connection
from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import FIELD_TYPES
from snowflake.connector.cursor import SnowflakeCursor

from . import __name__ as this_module
from .formats import Format
from .util import Command, Data, ExportFn, prettify, take


def main_sql(sqls: list[str], cmd: Command, **kwargs: Any) -> None:
    "run command against each SQL from the list"
    sqls_ = (s.rstrip(whitespace + ";") for s in sqls)

    @with_connection(getLogger(this_module))
    def go(cnx: SnowflakeConnection, **kwargs: Any):
        with cnx.cursor() as csr:
            for sql in sqls_:
                match cmd:
                    case Command.EXPORT:
                        _run(csr, sql=sql, **kwargs)
                    case Command.SHOW_SCHEMA:
                        _print_meta(csr, sql=sql)
                    case Command.SHOW_SQL:
                        pass


    if cmd == Command.SHOW_SQL:
        for sql in sqls_:
            print(sql + ";")
    else:
        go(**kwargs)  # type: ignore


def _print_meta(csr: SnowflakeCursor, sql: str, export: ExportFn = Format.default().export(), **_: Any):
    meta = csr.describe(sql)  # type: ignore

    headers = ["Name", "Type", "Size", "Prec", "Scale", "Nulls?"]
    types = [str, str, int, int, int, bool]
    data: Data = ((m.name, FIELD_TYPES[m.type_code].name, m.internal_size, m.precision, m.scale, m.is_nullable) for m in meta)

    export(data, headers, types)


def _run(
    csr: SnowflakeCursor,
    sql: str,
    export: ExportFn = Format.default().export(),
    limit: int | None = None,
    pretty_headers: bool = False,
) -> None:
    csr.execute(sql)
    if limit is not None and limit > 0 and csr.rowcount is not None and csr.rowcount > limit:
        csr.execute(f"select * from (table(result_scan())) limit {limit}")

    headers = prettify(m.name for m in csr.description) if pretty_headers else [m.name for m in csr.description]
    types = [pytype(d) for d in csr.description]
    data = take(cast(Data, csr)) if limit is None else cast(Data, csr)

    export(data, headers, types)


def run(
    csr: SnowflakeCursor,
    sql: str,
    format: Format = Format.default(),
    limit: int | None = None,
    file: Path | None = None,
    pretty_headers: bool = False,
) -> None:
    return _run(csr, sql, export=format.export(file), limit=limit, pretty_headers=pretty_headers)


def print_meta(csr: SnowflakeCursor, sql: str, format: Format = Format.default(), file: Path | None = None, **kwargs: Any):
    return _print_meta(csr, sql, export=format.export(file), **kwargs)
