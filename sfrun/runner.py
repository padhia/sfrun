"SQLRunner class to run a batch of SQL statement from a file"

import logging
import sys
from dataclasses import dataclass
from functools import partial
from io import StringIO
from itertools import islice
from pathlib import Path
from typing import Any, Iterable, TextIO, cast

from sfconn import pytype
from snowflake.connector import DatabaseError, SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.util_text import split_statements
from yappt import tabulate
from yappt.grid import AsciiBoxStyle

from .util import intersperse

logger = logging.getLogger(__name__)


@dataclass
class SqlRunner:
    """
    Run a batch of sqls, optionally conditioally stopping execution and outputing the results to a file
    """
    sqls: list[str]
    stop_on_error: bool = True
    pretty: bool = False
    limit: int = 500
    output: Path | None = None

    def run_sql(self, csr: SnowflakeCursor, input: str, output: TextIO) -> bool:
        "print, run, show results; returns True if no errors"

        def prettify_title(t: str) -> str:
            return t.replace("_", " ").title()

        def raw_title(t: str) -> str:
            return t

        mk_title = prettify_title if self.pretty else raw_title

        try:
            print(input.rstrip(), file=output)
            csr.execute(input)
        except DatabaseError as err:
            logger.error(err)
            return False

        headers = [mk_title(m.name) for m in csr.description]
        types = [pytype(d) for d in csr.description]
        rows = cast(Iterable[tuple[Any, ...]], csr)
        if self.limit > 0:
            rows = islice(rows, self.limit)

        tabulate(rows, headers=headers, types=types, default_grid_style=AsciiBoxStyle, file=output)

        return True

    def run(self, cnx: SnowflakeConnection) -> bool:
        """reads, parses and attempts to run all statments from file.

        Args:
            cnx: Snowflake connection

        Returns:
            True if all statements ran without error, False otherwise
        """
        def run_all(outf: TextIO) -> bool:
            with cnx.cursor() as csr:
                done = intersperse(partial(self.run_sql, csr, output=outf), self.sqls)
                if self.stop_on_error:
                    return all(done)

                return sum(1 for d in done if not d) == 0

        if self.output is None:
            return run_all(sys.stdout)

        with self.output.open("w") as outf:
            return run_all(outf)


class SqlScript(SqlRunner):
    """reads, parses and attempts to run all statments from file"""

    def __init__(self, input: Path | str | None, out_dir: Path | None = None, output_ext: str = "log", output: Path | None = None, **kwargs: Any):
        text = cast(str, sys.stdin.read()) if input is None else input if isinstance(input, str) else input.read_text()
        sqls = [stmt for stmt, _ in split_statements(StringIO(text.strip()), remove_comments=True)]

        if out_dir is not None and output is None and isinstance(input, Path):
            output = out_dir / f"{input.stem}.{output_ext}"

        return SqlRunner.__init__(self, sqls, output = output, **kwargs)
