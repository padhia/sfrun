"SQLRunner class to run a batch of SQL statement from a file"

import importlib.util as iu
import logging
import sys
from dataclasses import dataclass
from functools import cached_property, partial
from io import StringIO
from itertools import islice
from pathlib import Path
from typing import Any, Iterable, Iterator, NamedTuple, Protocol, Self, TextIO, cast

from sfconn import pytype
from snowflake.connector import DatabaseError, SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.util_text import split_statements
from snowflake.snowpark import DataFrame, Session
from yappt import tabulate
from yappt.grid import AsciiBoxStyle

from .formats import Format
from .util import SnowparkFn, intersperse, prettify, take

logger = logging.getLogger(__name__)


class Sql(NamedTuple):
    name: str | None
    text: str


class Input(Protocol):
    def __iter__(self) -> Iterator[Sql]: ...


@dataclass
class SQLScript(Input):
    path: Path | None

    def __iter__(self) -> Iterator[Sql]:
        name = None if self.path is None else self.path.stem
        text = cast(str, sys.stdin.read()) if self.path is None else self.path.read_text()
        yield from [Sql(name, stmt) for stmt, _ in split_statements(StringIO(text.strip()), remove_comments=True)]


@dataclass
class PYScript(Input):
    sess: Session
    path: Path
    fn_name: str = "main"

    @cached_property
    def fn(self) -> SnowparkFn:
        "load named Python script and return main() function"
        script = self.path.absolute()

        sys.path.insert(0, str(self.path.parent))

        spec = iu.find_spec(self.path.stem)
        if spec is None or spec.loader is None:
            raise TypeError(f'Script "{self.path.stem}" could not be loaded')

        module = iu.module_from_spec(spec)
        spec.loader.exec_module(module)

        try:
            f = getattr(module, self.fn_name)
        except AttributeError:
            raise ValueError(f"{script} does not contain '{self.fn_name}' function")

        if not callable(f):
            raise TypeError(f"'{self.fn_name}' ('{script}') is invalid; must be a function")

        sig = f.__annotations__
        if sig:  # if function has type hints, verify function has the right signature
            if "return" in sig and not issubclass(sig.pop("return"), (DataFrame, str)):
                raise TypeError(f"Invalid function '{self.fn_name}' ('{script}'); must return either a DataFrame or a str")
            if (inp_type := list(sig.values())) and not (len(inp_type) == 1 and issubclass(inp_type[0], Session)):
                raise TypeError(
                    f"Invalid function '{self.fn_name}' ('{script}'); must accept exacly one argument of type Session"
                )

        return cast(SnowparkFn, f)

    def __iter__(self) -> Iterator[Sql]:
        r = self.fn(self.sess)
        if isinstance(r, DataFrame):
            yield from [Sql(self.path.stem, q) for q in  r.queries["queries"]]
        else:
            yield Sql(self.path.stem, r)


@dataclass
class SqlList(Input):
    sqls: list[str]

    def __iter__(self) -> Iterator[Sql]:
        yield from (Sql(None, x) for x in self.sqls)


@dataclass
class SqlRunner:
    """
    Run a batch of sqls, optionally conditioally stopping execution and outputing the results to a file
    """

    inp: Input
    formatter: Format = Format.default()
    stop_on_error: bool = True
    pretty: bool = False
    limit: int = 500
    output: Path | None = None
    echo: bool = True

    def run_sql(self, csr: SnowflakeCursor, input: Sql, output: TextIO) -> bool:
        "print, run, show results; returns True if no errors"

        def prettify_title(t: str) -> str:
            return t.replace("_", " ").title()

        def raw_title(t: str) -> str:
            return t

        mk_title = prettify_title if self.pretty else raw_title

        try:
            if self.echo:
                print(input.text.rstrip(), file=output)
            csr.execute(input.text)
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
                done = intersperse(partial(self.run_sql, csr, output=outf), iter(self.inp))
                if self.stop_on_error:
                    return all(done)

                return sum(1 for d in done if not d) == 0

        if self.output is None:
            return run_all(sys.stdout)

        with self.output.open("w") as outf:
            return run_all(outf)

    @classmethod
    def make(
        cls: type[Self],
        input: Path | None | str | list[str],
        out_dir: Path | None = None,
        output_ext: str = "log",
        output: Path | None = None,
        **kwargs: Any,
    ) -> Self:
        if out_dir is not None and output is None and isinstance(input, Path):
            output = out_dir / f"{input.stem}.{output_ext}"

        if isinstance(input, Path) or input is None:
            inp = SQLScript(input)
        elif isinstance(input, str):
            inp = SqlList([input])
        else:
            inp = SqlList(input)

        return cls(inp, output=output, **kwargs)


class SqlScript(SqlRunner):
    """reads, parses and attempts to run all statments from file"""

    def __init__(
        self,
        input: Path | str | None,
        out_dir: Path | None = None,
        output_ext: str = "log",
        output: Path | None = None,
        **kwargs: Any,
    ):
        text = cast(str, sys.stdin.read()) if input is None else input if isinstance(input, str) else input.read_text()
        sqls = [stmt for stmt, _ in split_statements(StringIO(text.strip()), remove_comments=True)]

        if out_dir is not None and output is None and isinstance(input, Path):
            output = out_dir / f"{input.stem}.{output_ext}"

        return SqlRunner.__init__(self, sqls, output=output, **kwargs)


class PyScript:
    path: Path
    fn_name: str
    pretty: bool = False
    limit: int = 500
    formatter: Format
    output: Path | None = None

    @cached_property
    def fn(self) -> SnowparkFn:
        "load named Python script and return main() function"
        script = self.path.absolute()

        sys.path.insert(0, str(self.path.parent))

        spec = iu.find_spec(self.path.stem)
        if spec is None or spec.loader is None:
            raise TypeError(f'Script "{self.path.stem}" could not be loaded')

        module = iu.module_from_spec(spec)
        spec.loader.exec_module(module)

        try:
            f = getattr(module, self.fn_name)
        except AttributeError:
            raise ValueError(f"{script} does not contain '{self.fn_name}' function")

        if not callable(f):
            raise TypeError(f"'{self.fn_name}' ('{script}') is invalid; must be a function")

        sig = f.__annotations__
        if sig:  # if function has type hints, verify function has the right signature
            if "return" in sig and not issubclass(sig.pop("return"), (DataFrame, str)):
                raise TypeError(f"Invalid function '{self.fn_name}' ('{script}'); must return either a DataFrame or a str")
            if (inp_type := list(sig.values())) and not (len(inp_type) == 1 and issubclass(inp_type[0], Session)):
                raise TypeError(
                    f"Invalid function '{self.fn_name}' ('{script}'); must accept exacly one argument of type Session"
                )

        return cast(SnowparkFn, f)

    def run(self, sess: Session) -> bool:
        df = _df if isinstance(_df := self.fn(sess), DataFrame) else sess.sql(_df)
        if self.limit is not None and self.limit > 0:
            df = df.limit(self.limit)

        schema = df.schema
        headers = prettify(f.name for f in schema.fields) if self.pretty else [f.name for f in schema.fields]
        types = [pytype(f.datatype) for f in schema.fields]

        try:
            data = take(df.to_local_iterator()) if self.limit is None else df.to_local_iterator()
            self.formatter.export(self.output)(data, headers, types)
            return True
        except Exception:
            return False
