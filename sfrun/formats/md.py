"save SQL result-set in markdown table format"

from itertools import chain
from typing import Iterable, TextIO

from yappt.tabulate import aligned_seq_iter, formatted_seq_iter
from yappt.types import HAlign

from ..util import Data, textio


@textio
def export(rows: Data, headers: list[str], types: list[type], output: TextIO) -> None:
    """export rows with given metadata"""

    def emit(xs: Iterable[str]) -> None:
        print("| " + " | ".join(xs) + " |", file=output)

    def separator(align: HAlign, width: int) -> str:
        if align == HAlign.CENTER:
            return ":" + "-" * max(width - 2, 1) + ":"
        if align == HAlign.LEFT:
            return ":" + "-" * max(width - 1, 1)
        return "-" * max(width - 1, 1) + ":"

    meta, fseq = formatted_seq_iter(rows, types, headers)
    alignments = [m.alignment for m in meta]
    aseq = aligned_seq_iter(chain([[m.title for m in meta]], fseq), alignments)

    for e, r in enumerate(aseq):
        if e == 1:
            emit(separator(a, w) for a, w in zip(alignments, map(len, r)))
        emit(r)
