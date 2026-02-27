from __future__ import annotations

import os
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

    def get_energy(self) -> float | None:
        if len(
            energy_tasks := [
                task.energy for task in self.instances if task.energy is not None
            ]
        ) > 0 and len(self.instances) != len(energy_tasks):
            print(f"WARNING: Step {self.name} has some tasks with no energy report")
        return sum(energy_tasks) if len(energy_tasks) else None

    def get_duration(self) -> timedelta | None:
        return (
            (self.get_end() - self.get_start()) if self.get_end() is not None else None
        )

    def __str__(self):
        return f"{self.name}. Start: {self.get_start()}. End: {self.get_end()}"

    def get_locations(self) -> MutableSequence[str]:
        return list(
            {loc for task in self.instances if (loc := task.get_location()) is not None}
        )


class Task(Action):
    def __init__(
        self,
        start: timedelta,
        end: timedelta | None,
        deployment: str | None = None,
        service: str | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(start, end)
        self.name: str = name
        self.deployment: str | None = deployment
        self.service: str | None = service
        self.queue_times: MutableSequence[Action] = []
        self.energy: float | None = None
        self.status: TaskStatus = TaskStatus.COMPLETED
        self.transfer_inputs: MutableMapping[str, TransferData]

    def get_energy(self) -> float:
        return self.energy

    def get_location(self) -> str | None:
        return (
            os.path.join(self.deployment, self.service)
            if self.service
            else self.deployment
        )

    def get_queue_time(self) -> timedelta | None:
        return (
            sum(q.end_time - q.start_time for q in self.queue_times)
            if self.queue_times
            else None
        )

    def __str__(self) -> str:
        return f"{self.name} {self.start_time} {self.end_time} {self.get_location()}"


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

    def empty(self) -> bool:
        return len(self.steps) == 0
