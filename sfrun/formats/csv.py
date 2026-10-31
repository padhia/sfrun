"save SQL result-set in CSV format"

import csv

from .base import Exporter


class CsvExporter(Exporter, name="csv", help="CSV"):
    "export data in CSV format"

    sep = ","

    def export(self) -> None:
        with self.text_output() as file:
            w = csv.writer(file, delimiter=self.sep)
            w.writerow(self.headers)
            w.writerows(self.rows)


class TsvExporter(CsvExporter, name="tsv", help="TSV"):
    "export data in TSV format"

    sep = "\t"
