from __future__ import annotations

import os
import re
from collections.abc import MutableSequence

from viewer.core.entity import Step, Task, TransferData
from viewer.core.utils import str_to_datetime


def _get_copy_info(
    words: MutableSequence[str], transfer_completed: bool = False
) -> tuple[str, str, str, str]:
    offset = 1 if transfer_completed else 0
    src_path = words[5 + offset]
    if words[7 + offset] == "local":  # local to remote
        dst_path = words[10 + offset]
        src_location = words[7 + offset]
        dst_location = words[13 + offset]
    elif len(words) > 11 + offset and words[12 + offset] == "local":  # remote to local
        dst_path = words[10 + offset]
        src_location = words[8 + offset]
        dst_location = words[12 + offset]
    elif (
        words[4 + offset] == "from" and words[6 + offset] == "to"
    ):  # location A to location A (local to local or RemoteA to RemoteA)
        dst_path = words[7 + offset]
        if words[9 + offset] == "local":
            src_location = words[9 + offset]
            dst_location = words[9 + offset]
        else:
            src_location = words[10 + offset]
            dst_location = words[10 + offset]
    else:  # remote A to remote B
        dst_path = words[9 + offset]
        src_location = words[7 + offset]
        dst_location = words[12 + offset]
    return src_location, src_path, dst_location, dst_path


def get_metadata_from_log(filepath: str):
    workflow_start, workflow_end, workflow_name = (None for _ in range(3))
    deployments = []
    file_copies = {}
    steps = {}
    last_timestamp = None
    queue_manager_ids = {}
    queue_locations = {}
    job_inputs_interval = {}
    job_input_reading = False
    job_input_name = None
    with open(filepath) as fd:
        for line in fd:
            words = [w.strip() for w in line.split(" ") if w]
            sentence = " ".join(words)
            if job_input_reading:
                if sentence == "}":
                    job_input_reading = False
                job_inputs_interval[job_input_name] += sentence
            elif re.match(r".*Job .* inputs: ", sentence):
                job_input_name = words[4]
                job_inputs_interval[job_input_name] = "{"
                job_input_reading = True
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
                file_copies[dst_path] = TransferData(
                    src_path=src_path,
                    dst_path=dst_path,
                    src_location=src_location,
                    dst_location=dst_location,
                    start=str_to_datetime(" ".join(words[:2])),
                )
            elif "EXECUTING step" in sentence:
                step = steps.setdefault(words[5], Step(words[5], []))
                step.instances.append(
                    Task(
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
                            instance.end_time = end_time - workflow_start
                            if workflow_end is None or workflow_end < end_time:
                                workflow_end = end_time
                            break
            elif re.match(r".*COMPLETED Step.*", sentence):
                step = steps.get(words[-1], None)
                for instance in step.instances if step is not None else []:
                    if instance.end_time is None:
                        instance.end_time = (
                            str_to_datetime(" ".join(words[:2])) - workflow_start
                        )
            elif re.match(r".*Scheduled job .* with job id .*", sentence):
                if os.path.dirname(words[5]) in steps.keys():
                    for instance in steps[os.path.dirname(words[5])].instances:
                        if instance.name == words[5]:
                            if instance.location in queue_locations.keys():
                                queue_start = queue_locations[instance.location][
                                    words[9]
                                ]["queue_starttime"]
                                queue_end = queue_locations[instance.location][
                                    words[9]
                                ]["queue_endtime"]
                                instance.queue_times.append((queue_start, queue_end))
                            else:
                                queue_manager_ids.setdefault(
                                    instance.location, []
                                ).append(words[9])
            try:
                last_timestamp = str_to_datetime(" ".join(words[:2]))
            except Exception:
                pass
    if workflow_end is None:
        workflow_end = last_timestamp
        error_end = workflow_end - workflow_start
        for step in steps.values():
            for instance in step.instances:
                if instance.end_time is None:
                    instance.end_time = error_end

    for copy_info in file_copies.values():
        print(
            f"src_path: {copy_info.src_path}\n"
            f"src_loc: {copy_info.src_location}\n"
            f"dst_path: {copy_info.dst_path}\n"
            f"dst_loc: {copy_info.dst_location}\n"
            f"transfer: {copy_info.end_time - copy_info.start_time}\n"
        )
        print("#" * 20)
    return (
        sorted(steps.values(), key=lambda x: x.get_start()),
        workflow_start,
        workflow_end,
    )
