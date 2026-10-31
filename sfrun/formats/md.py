"save SQL result-set in markdown table format"

from collections.abc import Iterable
from itertools import chain

from yappt.tabulate import aligned_seq_iter, formatted_seq_iter
from yappt.types import HAlign

from .base import Exporter


def separator(align: HAlign, width: int) -> str:
    if align == HAlign.CENTER:
        return ":" + "-" * max(width - 2, 1) + ":"
    if align == HAlign.LEFT:
        return ":" + "-" * max(width - 1, 1)
    return "-" * max(width - 1, 1) + ":"


class MdExporter(Exporter, name="md", help="markdown"):
    """export rows as a markdown table"""

    def export(self) -> None:
        with self.text_output() as output:

            def emit(xs: Iterable[str]) -> None:
                print("| " + " | ".join(xs) + " |", file=output)

            meta, fseq = formatted_seq_iter(self.rows, self.types, self.headers)
            alignments = [m.alignment for m in meta]
            aseq = aligned_seq_iter(chain([[m.title for m in meta]], fseq), alignments)

            for e, r in enumerate(aseq):
                if e == 1:
                    emit(separator(a, w) for a, w in zip(alignments, map(len, r)))
                emit(r)
