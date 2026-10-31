"save SQL result-set in JSON format"

import json
from collections.abc import Iterable
from typing import Any

from yappt import iter_more

from ..util import Data
from .base import Exporter


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


class JsonExporter(Exporter, name="json", help="JSON"):
    """export rows as JSON"""

    def export(self) -> None:
        with self.text_output() as output:
            for x in as_json_array(iter_row(self.rows, self.headers)):
                print(x, file=output)
