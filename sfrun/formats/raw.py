from .base import Exporter


class RawExporter(Exporter, name="raw", help="raw (no headers, tab delimited)"):
    """export data in raw format (textual, tab delimited)"""

    def export(self) -> None:
        with self.text_output() as file:
            for r in self.rows:
                print("\t".join(str(c) for c in r), file=file)
