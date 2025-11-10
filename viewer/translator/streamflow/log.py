from __future__ import annotations

import os
import re
from datetime import datetime

from viewer.core.entity import Instance, Step
from viewer.core.utils import str_to_datetime


def _get_copy_info(words, transfer_completed=False):
    offset = 1 if transfer_completed else 0
    src_path = words[4 + offset]
    if words[6 + offset] == "local":  # local to remote
        dst_path = words[9 + offset]
        src_location = words[6 + offset]
        dst_location = words[12 + offset]
    elif words[4 + offset] == "from":
        dst_path = words[7 + offset]
        src_location = words[9 + offset]
        dst_location = words[9 + offset]
    elif words[11 + offset] == "local":  # remote to local
        dst_path = words[9 + offset]
        src_location = words[7 + offset]
        dst_location = words[11 + offset]
    else:  # remote to remote
        dst_path = words[9 + offset]
        src_location = words[7 + offset]
        dst_location = words[12 + offset]
    return src_location, src_path, dst_location, dst_path


class CopyInfo:
    def __init__(
        self,
        src_path: str,
        dst_path: str,
        src_location: str,
        dst_location: str,
        start_time: datetime,
    ):
        self.src_path: str = src_path
        self.dst_path: str = dst_path
        self.src_location: str = src_location
        self.dst_location: str = dst_location
        self.start_time: datetime = start_time
        self.end_time: datetime | None = None


def get_metadata_from_log(filepath):
    workflow_start, workflow_end, workflow_name = (None for _ in range(3))
    deployments = []
    file_copies = {}
    steps = {}
    last_timestamp = None
    with open(filepath) as fd:
        for line in fd:
            words = [w.strip() for w in line.split(" ") if w]
            sentence = " ".join(words)
            if workflow_start is None and "Processing" in words and "workflow" in words:
                workflow_start = str_to_datetime(" ".join(words[:2]))
            elif "DEPLOYING" in words:
                deployments.append(words[-1])
            elif len(words) > 3 and "COMPLETED" == words[3] and "copy" == words[4]:
                src_location, src_path, dst_location, dst_path = _get_copy_info(
                    words, transfer_completed=True
                )
                copy_info = file_copies[dst_path]
                copy_info.end_time = str_to_datetime(" ".join(words[:2]))
                if (
                    src_path != copy_info.src_path
                    or dst_path != copy_info.dst_path
                    or src_location != copy_info.src_location
                    or dst_location != copy_info.dst_location
                ):
                    raise Exception("Error copy scraping start and end times")
            elif len(words) > 3 and "COPYING" in words[3]:
                src_location, src_path, dst_location, dst_path = _get_copy_info(words)
                file_copies[dst_path] = CopyInfo(
                    src_path=src_path,
                    dst_path=dst_path,
                    src_location=src_location,
                    dst_location=dst_location,
                    start_time=str_to_datetime(" ".join(words[:2])),
                )
            elif "EXECUTING step" in sentence:
                step = steps.setdefault(words[5], Step(words[5], []))
                step.instances.append(
                    Instance(
                        start=str_to_datetime(" ".join(words[:2])) - workflow_start,
                        end=None,
                        location=words[10],
                        name=words[7][:-1],
                    )
                )
            elif re.match(r".*Job .* changed status to COMPLETED", sentence):
                if os.path.dirname(words[4]) in steps.keys():
                    for instance in steps[os.path.dirname(words[4])].instances:
                        if instance.name == words[4]:
                            end_time = str_to_datetime(" ".join(words[:2]))
                            instance.end = end_time - workflow_start
                            if workflow_end is None or workflow_end < end_time:
                                workflow_end = end_time
                            break
            elif re.match(r".*COMPLETED Step.*", sentence):
                step = steps.get(words[-1], None)
                for instance in step.instances if step is not None else []:
                    if instance.end is None:
                        instance.end = (
                            str_to_datetime(" ".join(words[:2])) - workflow_start
                        )
            try:
                last_timestamp = str_to_datetime(" ".join(words[:2]))
            except Exception:
                pass
    if workflow_end is None:
        workflow_end = last_timestamp
        error_end = workflow_end - workflow_start
        for step in steps.values():
            for instance in step.instances:
                if instance.end is None:
                    instance.end = error_end
    return (
        sorted(steps.values(), key=lambda x: x.get_start()),
        workflow_start,
        workflow_end,
    )
