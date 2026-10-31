"""save output of SQL as formatted table"""

from yappt import tabulate

from .base import Exporter


class FmtExporter(Exporter, name="fmt", help="tabular", default=True):
    "export formatted data"

    def export(self) -> None:
        with self.text_output() as file:
            tabulate(self.rows, headers=self.headers, types=[t | None for t in self.types], file=file)  # type: ignore
