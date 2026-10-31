"base class for all output formats"

import sys
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import ClassVar, TextIO

from ..util import Data


class Exporter(ABC):
    "export a result-set, optionally to a file, in a specific format"

    registry: ClassVar[dict[str, type["Exporter"]]] = {}
    _default: ClassVar[type["Exporter"] | None] = None

    name: ClassVar[str]
    help: ClassVar[str]
    extensions: ClassVar[tuple[str, ...]]
    requires_file: ClassVar[bool]

    def __init_subclass__(
        cls,
        name: str | None = None,
        help: str = "",
        extensions: tuple[str, ...] = (),
        requires_file: bool = False,
        default: bool = False,
        **kwargs: object,
    ) -> None:
        super().__init_subclass__(**kwargs)
        if name is None:  # abstract intermediate classes are not registered
            return

        cls.name, cls.help, cls.requires_file = name, help, requires_file
        cls.extensions = extensions or (name,)
        Exporter.registry[name] = cls
        if default:
            Exporter._default = cls

    def __init__(self, rows: Data, headers: list[str], types: list[type], file: Path | None = None) -> None:
        if self.requires_file and file is None:
            raise ValueError(f"file cannot be None when exporting in '{self.name}' format")

        self.rows = rows
        self.headers = headers
        self.types = types
        self.file = file

    @abstractmethod
    def export(self) -> None:
        "write rows in this format"

    @contextmanager
    def text_output(self) -> Iterator[TextIO]:
        "text stream to write to: the output file when one was given, stdout otherwise"
        if self.file is None:
            yield sys.stdout
        else:
            with self.file.open("w") as f:
                yield f

    @classmethod
    def by_name(cls, name: str) -> type["Exporter"]:
        return cls.registry[name]

    @classmethod
    def default(cls) -> type["Exporter"]:
        assert Exporter._default is not None, "no default format registered"
        return Exporter._default

    @classmethod
    def from_path(cls, path: Path) -> type["Exporter"]:
        "guess format from file extension; fall back to the default format if unrecognized"
        ext = path.suffix.lower().removeprefix(".")
        return next((c for c in cls.registry.values() if ext in c.extensions), cls.default())
