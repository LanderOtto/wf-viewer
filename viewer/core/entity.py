from __future__ import annotations

from collections.abc import MutableMapping, MutableSequence
from datetime import datetime, timedelta
from enum import Enum


class TaskStatus(Enum):
    COMPLETED = "completed"
    FAILED = "failed"


class Action:
    def __init__(self, start_time: timedelta, end_time: timedelta | None) -> None:
        self.start_time: timedelta = start_time
        self.end_time: timedelta | None = end_time

    def get_duration(self) -> timedelta:
        return self.end_time - self.start_time


class Step:
    def __init__(self, name: str, instances: MutableSequence[Task]):
        self.name: str = name
        self.instances: MutableSequence[Task] = instances

    def get_start(self) -> timedelta:
        return min(instance.start_time for instance in self.instances)

    def get_end(self) -> timedelta | None:
        if times := [
            instance.end_time
            for instance in self.instances
            if instance.end_time is not None
        ]:
            return max(times)
        else:
            return None

    def get_duration(self) -> timedelta | None:
        return (
            (self.get_end() - self.get_start()) if self.get_end() is not None else None
        )

    def __str__(self):
        return f"{self.name}. Start: {self.get_start()}. End: {self.get_end()}"


class Task(Action):
    def __init__(
        self,
        start: timedelta,
        end: timedelta | None,
        location: str | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(start, end)
        self.name = name
        self.location = location
        self.queue_times: MutableSequence[Action] = []
        self.status: TaskStatus = TaskStatus.COMPLETED
        self.transfer_inputs: MutableMapping[str, TransferData]

    def __str__(self) -> str:
        return f"{self.name} {self.start_time} {self.end_time} {self.location}"


class TransferData(Action):
    def __init__(
        self,
        src_path: str,
        src_location: str,
        dst_path: str,
        dst_location: str,
        start: timedelta,
        end: timedelta | None = None,
    ) -> None:
        super().__init__(start, end)
        self.src_path: str = src_path
        self.src_location: str = src_location
        self.dst_path: str = dst_path
        self.dst_location: str = dst_location


class Workflow:
    def __init__(self, start_date: datetime, end_date: datetime) -> None:
        self.start_date: datetime = start_date
        self.end_date: datetime = end_date
        self.start_time: timedelta = start_date - start_date
        self.end_time: timedelta = end_date - start_date
        self.steps: MutableSequence[Step] = []
