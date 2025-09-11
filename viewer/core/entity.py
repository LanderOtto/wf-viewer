from __future__ import annotations

from datetime import timedelta
from typing import MutableSequence


class Instance:
    def __init__(
        self,
        start: timedelta,
        end: timedelta | None,
        location: str | None = None,
        name: str | None = None,
    ):
        self.start: timedelta = start
        self.end: timedelta | None = end
        self.name = name
        self.location = location

    def get_exec(self) -> timedelta | None:
        return (self.end - self.start) if self.end is not None else None

    def __str__(self):
        return f"{self.name} {self.start} {self.end} {self.location}"


class Step:
    def __init__(self, name: str, instances: MutableSequence[Instance]):
        self.name: str = name
        self.instances: MutableSequence[Instance] = instances

    def get_start(self) -> timedelta:
        return min(instance.start for instance in self.instances)

    def get_end(self) -> timedelta | None:
        if times := [
            instance.end for instance in self.instances if instance.end is not None
        ]:
            return max(times)
        else:
            return None

    def get_exec(self) -> timedelta | None:
        return (
            (self.get_end() - self.get_start()) if self.get_end() is not None else None
        )

    def __str__(self):
        return f"{self.name}. Start: {self.get_start()}. End: {self.get_end()}"
