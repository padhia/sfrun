"save SQL result-set in JSON format"

import json
from typing import Any, Iterable, TextIO

from yappt import iter_more

from ..util import Data, textio


def iter_row(rows: Data, headers: list[str]) -> Iterable[dict[str, Any]]:
    def to_json(v: Any) -> str | int | float | bool | None:
        return v if v is None or isinstance(v, str | int | float | bool) else str(v)

    def as_dict(xs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
        return {h: to_json(v) for h, v in xs}

    yield from (as_dict(zip(headers, row)) for row in rows)


def as_json_array(rows: Iterable[dict[str, Any]]) -> Iterable[str]:
    yield "["
    for more, row in iter_more(rows):
        doc = "\n".join("  " + y for y in json.dumps(row, indent=2).splitlines())
        yield doc + ("," if more else "")
    yield "]"


@textio
def export(rows: Data, headers: list[str], types: list[type], output: TextIO) -> None:
    """export rows as JSON"""
    for x in as_json_array(iter_row(rows, headers)):
        print(x, file=output)
