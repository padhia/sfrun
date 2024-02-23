"save SQL result-set in CSV format"
import csv
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import IO, Any, Iterable, Optional


def export(
    rows: Iterable[tuple[Any, ...]],
    headers: list[str],
    _: list[type],
    output: Optional[Path] = None,
    write_header: bool = True,
    sep: str = ",",
) -> None:
    "output cursor result to CSV file"

    def write_file(csvfile: IO[str]) -> None:
        w = csv.writer(csvfile, delimiter=sep)
        if write_header:
            w.writerow(headers)
        w.writerows(rows)

    if output is not None:
        with output.open("w", newline="") as f:
            write_file(f)
    else:
        write_file(sys.stdout)


def add_args(parser: ArgumentParser) -> Any:
    """parse user supplied args"""

    def num_chr(s: str) -> str:
        try:
            return chr(int(s))
        except ValueError:
            return s

    parser.add_argument("output", metavar="FILE", nargs="?", type=Path, help="output file, default stdout")

    x = parser.add_mutually_exclusive_group()
    x.add_argument("--sep", default=",", type=num_chr, help="field separator (default ,)")
    x.add_argument("--tsv", const="\t", action="store_const", dest="sep", help="Use tabs as field separators")

    parser.add_argument("-H", "--no-headers", action="store_false", dest="write_header", help="do not write header")
