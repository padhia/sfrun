"save SQL result-set in CSV format"

import csv
import sys
from functools import partial
from typing import TextIO

from ..util import Data, textio


def export(rows: Data, headers: list[str], types: list[type], file: TextIO = sys.stdout, sep: str = ",") -> None:
    "export data in CSV format"
    w = csv.writer(file, delimiter=sep)
    w.writerow(headers)
    w.writerows(rows)


csv_export = textio(partial(export, sep=","))
tsv_export = textio(partial(export, sep="\t"))
