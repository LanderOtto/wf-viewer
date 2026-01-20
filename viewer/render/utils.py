from __future__ import annotations

import sys
from collections.abc import MutableSequence


def multi_print(
    *args,
    sep: str = " ",
    end: str = "\n",
    file_descriptors: MutableSequence[int] | None = None,
) -> None:
    if file_descriptors is None:
        file_descriptors = [sys.stdout]
    for fd in file_descriptors:
        print(*args, sep=sep, end=end, file=fd)


def print_split_section(
    distance: int = 1,
    hash: int = 40,
    file_descriptors: MutableSequence[int] | None = None,
) -> None:
    multi_print("\n" * distance, end="", file_descriptors=file_descriptors)
    multi_print("#" * hash, file_descriptors=file_descriptors)
