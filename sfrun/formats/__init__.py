from argparse import ArgumentParser, ArgumentTypeError
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Self

from ..util import Data, ExportFn
from . import csv, fmt, json, jsonl, md, raw, xls


class Format(str, Enum):
    FMT = "fmt"
    CSV = "csv"
    TSV = "tsv"
    RAW = "raw"
    MD = "md"
    XLS = "xls"
    JSON = "json"
    JSONL = "jsonl"

    def arg_help(self) -> str:
        match self:
            case self.FMT:
                return "tabular"
            case self.CSV:
                return "CSV"
            case self.TSV:
                return "TSV"
            case self.MD:
                return "markdown"
            case self.RAW:
                return "raw (no headers, tab delimited)"
            case self.XLS:
                return "MS Excel"
            case self.JSON:
                return "JSON"
            case self.JSONL:
                return "jsonline"

    @property
    def _export(self) -> Callable[[Data, list[str], list[type], Path | None], None]:
        match self:
            case self.FMT:
                return fmt.export
            case self.CSV:
                return csv.csv_export
            case self.TSV:
                return csv.tsv_export
            case self.MD:
                return md.export
            case self.RAW:
                return raw.export
            case self.XLS:
                return xls.export  # type: ignore
            case self.JSON:
                return json.export
            case self.JSONL:
                return jsonl.export

    def export(self, file: Path | None = None) -> ExportFn:
        def wrapped(data: Data, headers: list[str], types: list[type]) -> None:
            return self._export(data, headers, types, file)

        if self._export is xls.export:
            if file is None:
                raise ValueError("file cannot be None when exporting in MS Excel format")

        return wrapped

    def export_arg(self, v: str) -> ExportFn:
        path = Path(v)
        if path.exists():
            if not path.is_file():
                raise ArgumentTypeError(f"Output file '{v}' is not a file")
        if not (p := path.parent).exists():
            raise ArgumentTypeError(f"Cannot create output file '{v}', the parent directory '{p}' does not exist")

        return self.export(path)

    @classmethod
    def default(cls: type[Self]) -> Self:
        return cls.FMT  # type: ignore

    @classmethod
    def add_args(cls: type[Self], parser: ArgumentParser) -> ArgumentParser:
        g = parser.add_argument_group("output formatting options")
        x = g.add_mutually_exclusive_group()

        for opt in cls:
            path_args: dict[str, Any] = {} if opt == Format.XLS else dict(nargs="?", const=opt.export(None))

            x.add_argument(
                f"--{opt.value}",
                dest="export",
                metavar="FILE",
                type=opt.export_arg,
                default=cls.default().export(),
                help=f"output in {opt.arg_help()} format",
                **path_args
            )

        return parser
