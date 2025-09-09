"Snwoflake DataFrame runner; print output in different formats"

import importlib.util as iu
import sys
from logging import getLogger
from pathlib import Path
from typing import Any, Iterable, cast

from sfconn import pytype, with_session
from snowflake.snowpark import DataFrame, Session

from . import __name__ as this_module
from .formats import Format
from .util import Command, ExportFn, SnowparkFn, prettify, take


def main_py(file: list[Path], fn: str, **kwargs: Any) -> None:
    @with_session(getLogger(this_module))
    def go(session: Session, fs: Iterable[SnowparkFn], cmd: Command, **kwargs: Any) -> None:
        for f in fs:
            df = _df if isinstance(_df := f(session), DataFrame) else session.sql(_df)
            match cmd:
                case Command.EXPORT:
                    _run(df, **kwargs)
                case Command.SHOW_SQL:
                    show_sql(df)
                case Command.SHOW_SCHEMA:
                    _print_meta(df)

    try:
        fs = (load_py(f, fn=fn) for f in file)
    except ImportError as err:
        raise SystemExit(str(err))

    return go(fs=fs, **kwargs)  # type: ignore


def _print_meta(df: DataFrame, export: ExportFn = Format.default().export(), **_: Any):
    schema = df.schema

    headers = ["Name", "Type", "Nulls?"]
    types = [str, str, bool]
    data = [(f.name, f.datatype, f.nullable) for f in schema.fields]

    export(data, headers, types)


def show_sql(df: DataFrame) -> None:
    for q in df.queries["queries"] + df.queries["post_actions"]:
        print(q + ";")


def _run(
    df: DataFrame,
    export: ExportFn = Format.default().export(),
    limit: int | None = None,
    pretty_headers: bool = False,
) -> None:
    if limit is not None and limit > 0:
        df = df.limit(limit)

    schema = df.schema
    field_names = [f.name[1:-1] if f.name.startswith('"') else f.name for f in schema.fields]
    types = [pytype(f.datatype) for f in schema.fields]
    data = take(df.to_local_iterator()) if limit is None else df.to_local_iterator()

    export(data, prettify(field_names) if pretty_headers else field_names, types)


def run(
    df: DataFrame,
    format: Format = Format.default(),
    limit: int | None = None,
    file: Path | None = None,
    pretty_headers: bool = False,
) -> None:
    return _run(df, export=format.export(file), limit=limit, pretty_headers=pretty_headers)


def print_meta(df: DataFrame, format: Format = Format.default(), file: Path | None = None, **kwargs: Any):
    return _print_meta(df, export=format.export(file), **kwargs)


def load_py(script: Path, fn: str = "main") -> SnowparkFn:
    "load named Python script and return main() function"
    script = script.absolute()

    sys.path.insert(0, str(script.parent))

    spec = iu.find_spec(script.stem)
    if spec is None or spec.loader is None:
        raise TypeError(f'Script "{script.stem}" could not be loaded')

    module = iu.module_from_spec(spec)
    spec.loader.exec_module(module)

    try:
        f = getattr(module, fn)
    except AttributeError:
        raise ValueError(f"{script} does not contain '{fn}' function")

    if not callable(f):
        raise TypeError(f"'{fn}' ('{script}') is invalid; must be a function")

    sig = f.__annotations__
    if sig:  # if function has type hints, verify function has the right signature
        if "return" in sig and not issubclass(sig.pop("return"), (DataFrame, str)):
            raise TypeError(f"Invalid function '{fn}' ('{script}'); must return either a DataFrame or a str")
        if (inp_type := list(sig.values())) and not (len(inp_type) == 1 and issubclass(inp_type[0], Session)):
            raise TypeError(f"Invalid function '{fn}' ('{script}'); must accept exacly one argument of type Session")

    return cast(SnowparkFn, f)
