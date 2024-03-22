"save SQL result-set in CSV format"

import csv
import sys
from typing import Any, Iterable, TextIO


def csv_export(rows: Iterable[tuple[Any, ...]], headers: list[str], _: list[type], output: TextIO = sys.stdout) -> None:
    export(rows, headers, output, ",")


def tsv_export(rows: Iterable[tuple[Any, ...]], headers: list[str], _: list[type], output: TextIO = sys.stdout) -> None:
    export(rows, headers, output, "\t")


def export(rows: Iterable[tuple[Any, ...]], headers: list[str], output: TextIO = sys.stdout, sep: str = ",") -> None:
    "output cursor result to CSV file"

    w = csv.writer(output, delimiter=sep)
    w.writerow(headers)
    w.writerows(rows)
