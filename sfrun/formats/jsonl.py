"save SQL result-set in jsonl format"

import json
from typing import TextIO

from ..util import Data, textio
from .json import iter_row


@textio
def export(rows: Data, headers: list[str], types: list[type], output: TextIO) -> None:
    """export rows as JSON"""
    for x in iter_row(rows, headers):
        print(json.dumps(x), file=output)
