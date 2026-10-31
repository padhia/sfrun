"save SQL result-set in jsonl format"

import json

from .base import Exporter
from .json import iter_row


class JsonlExporter(Exporter, name="jsonl", help="jsonline"):
    """export rows as JSON lines"""

    def export(self) -> None:
        with self.text_output() as output:
            for x in iter_row(self.rows, self.headers):
                print(json.dumps(x), file=output)
