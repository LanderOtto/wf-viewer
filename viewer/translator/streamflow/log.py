from __future__ import annotations

import os
import posixpath
import re
from collections.abc import MutableMapping, MutableSequence
from pathlib import PurePath
from typing import Any

from viewer.core.entity import Action, Step, Task, Workflow
from viewer.core.utils import str_to_datetime


class FileNode:
    def __init__(self, name: str):
        self.name: str = name
        self.children: MutableMapping[str, FileNode] = {}

    def add_child(self, name: str) -> FileNode:
        if name not in self.children:
            self.children[name] = FileNode(name)
        return self.children[name]

    def get_child(self, name: str) -> FileNode | None:
        return self.children.get(name)


class FileSystem:
    def __init__(self, name: str):
        self.name: str = name
        self.root: FileNode = FileNode(posixpath.sep)

    def add(self, path: str):
        if not (p := PurePath(path)).is_absolute():
            raise ValueError(f"Path {p} must be absolute.")
        curr_node = self.root
        for part in p.parts[1:]:
            curr_node = curr_node.add_child(part)


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
    return (
        src_location.split(os.sep)[0],
        src_path,
        dst_location.split(os.sep)[0],
        dst_path,
    )


def translate_log(
    filepath: str, location_metadata: MutableMapping[str, Any]
) -> Workflow:
    workflow_start, workflow_end, workflow_name = (None for _ in range(3))
    deployments = []
    # file_copies = {}
    steps = {}
    last_timestamp = None
    unknown_jobs_info = {}
    job_inputs_interval = {}
    job_input_reading = False
    job_input_name = None
    filesystems = {"local": FileSystem("local")}
    with open(filepath) as fd:
        for line in fd:
            words = [w.strip() for w in line.split(" ") if w]
            sentence = " ".join(words)
            if job_input_reading:
                if sentence == "}":
                    job_input_reading = False
                job_inputs_interval[job_input_name] += sentence
            elif match := re.search(
                r"^(?P<timestamp>[\d-]+\s[\d:.]+)\s+DEBUG\s+Job\s+(?P<job_name>\S+)\s+inputs:\s+\{$",
                sentence,
            ):
                job_input_name = match.group("job_name")
                job_inputs_interval[job_input_name] = "{"
                job_input_reading = True
            if match := re.search(
                r"^(?P<timestamp>[\d-]+\s[\d:.]+)\s+INFO\s+Processing\s+workflow\s+(?P<workflow_id>[\w-]+)$",
                sentence,
            ):
                if workflow_start is not None:
                    raise Exception("There are multiple workflows in the log")
                workflow_start = str_to_datetime(match.group("timestamp"))
            elif match := re.search(
                r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+INFO\s+DEPLOYING\s+(?P<deployment>\S+)$",
                sentence,
            ):
                deployment = match.group("deployment")
                deployments.append(deployment)
                filesystems[deployment] = FileSystem(deployment)
            # elif len(words) > 3 and "COMPLETED" == words[3] and "copy" == words[4]:
            #     src_location, src_path, dst_location, dst_path = _get_copy_info(
            #         words, transfer_completed=True
            #     )
            #     filesystems[src_location].add(src_path)
            #     filesystems[dst_location].add(dst_path)
            #     copy_info = file_copies[dst_path]
            #     copy_info.end_time = str_to_datetime(" ".join(words[:2]))
            #     if (
            #         src_path != copy_info.src_path
            #         or dst_path != copy_info.dst_path
            #         or src_location != copy_info.src_location
            #         or dst_location != copy_info.dst_location
            #     ):
            #         raise Exception("Error copy scraping start and end times")
            # elif len(words) > 3 and "COPYING" in words[3]:
            #     src_location, src_path, dst_location, dst_path = _get_copy_info(words)
            #     file_copies[dst_path] = TransferData(
            #         src_path=src_path,
            #         dst_path=dst_path,
            #         src_location=src_location,
            #         dst_location=dst_location,
            #         start=str_to_datetime(" ".join(words[:2])),
            #     )
            elif match := re.search(
                r"^(?P<timestamp>[\d-]+\s[\d:.]+)\s+INFO\s+EXECUTING\s+step\s+(?P<step_name>\S+)\s+\(job\s+(?P<job_name>\S+)\)\s+(?:on\s+location\s+)?(?P<execution_type>\S+)\s+into\s+directory\s+(?P<directory>.*?):?$",
                sentence,
            ):
                step_name = match.group("step_name")
                step = steps.setdefault(step_name, Step(step_name, []))
                if (location := match.group("execution_type")) == "locally":
                    deployment = "local"
                    service = None
                else:
                    loc_components = location.split(os.sep)
                    deployment = loc_components[0]
                    if len(loc_components) > 2:
                        if len(loc_components) > 3:
                            print(
                                f"WARNING: Location {location} wraps another deployment"
                            )
                        service = loc_components[1]
                    else:
                        service = None
                step.instances.append(
                    Task(
                        start=str_to_datetime(match.group("timestamp"))
                        - workflow_start,
                        end=None,
                        deployment=deployment,
                        service=service,
                        name=match.group("job_name"),
                    )
                )
            elif match := re.search(
                r"^(?P<timestamp>[\d-]+\s[\d:.]+)\s+DEBUG\s+Job\s+(?P<job_name>\S+)\s+changed\s+status\s+to\s+(?P<status>\S+)$",
                sentence,
            ):
                job_name = match.group("job_name")
                if os.path.dirname(job_name) in steps.keys():
                    for instance in steps[os.path.dirname(job_name)].instances:
                        if instance.name == job_name:
                            end_time = str_to_datetime(match.group("timestamp"))
                            instance.end_time = end_time - workflow_start
                            if workflow_end is None or workflow_end < end_time:
                                workflow_end = end_time
                            break
            elif match := re.search(
                r"^(?P<timestamp>[\d-]+\s[\d:.]+)\s+INFO\s+COMPLETED\s+Step\s+(?P<step_name>\S+)$",
                sentence,
            ):
                step = steps.get(match.group("step_name"), None)
                missing_log = True
                for instance in step.instances if step is not None else []:
                    if instance.end_time is None:
                        missing_log = False
                        instance.end_time = (
                            str_to_datetime(match.group("timestamp")) - workflow_start
                        )
                if missing_log:
                    print(
                        f"WARNING: The step {step.name} completed, but the termination logs for some instances are missing. "
                        "A parsing error likely occurred. "
                        "(Note: StreamFlow log in debug mode is required to retrieve all necessary information."
                    )
            elif match := re.search(
                r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+INFO\s+Scheduled job (?P<streamflow_job>[\w\-/]+) with job id (?P<slurm_job>\d+)",
                sentence,
            ):
                streamflow_job = match.group("streamflow_job")
                if (step_name := os.path.dirname(streamflow_job)) in steps.keys():
                    for instance in steps[step_name].instances:
                        if instance.name == streamflow_job:
                            slurm_job = int(match.group("slurm_job"))
                            if instance.deployment in location_metadata.keys():
                                queue_start = location_metadata[instance.deployment][
                                    slurm_job
                                ]["queue_starttime"]
                                queue_end = location_metadata[instance.deployment][
                                    slurm_job
                                ]["queue_endtime"]
                                instance.queue_times.append(
                                    Action(queue_start, queue_end)
                                )
                                instance.energy = location_metadata[
                                    instance.deployment
                                ][slurm_job]["avg_energy"]
                            else:
                                unknown_jobs_info.setdefault(
                                    instance.deployment, []
                                ).append(str(slurm_job))
            try:
                if (tmp_timestamp := str_to_datetime(" ".join(words[:2]))) is not None:
                    last_timestamp = tmp_timestamp
            except Exception:
                pass
    if workflow_end is None:
        print(
            "WARNING: the workflow end time is missing. "
            "A parsing error likely occurred. "
            "(Note: StreamFlow log in debug mode is required to retrieve all necessary information)."
        )
        workflow_end = last_timestamp
        error_end = workflow_end - workflow_start
        missing_terminations = True
        for step in steps.values():
            for instance in step.instances:
                if instance.end_time is None:
                    missing_terminations = False
                    instance.end_time = error_end
        if missing_terminations:
            print(
                "WARNING: Some task end times are missing. The step's end time has been set, but it is inaccurate. "
                "(Note: StreamFlow log in debug mode is required to retrieve all necessary information."
            )

    if unknown_jobs_info:
        print(
            "WARNING: Missing jobs info in some locations execute the following command in the locations"
        )
        for loc, jobs in unknown_jobs_info.items():
            print(
                f"Location {loc}: `sacct --json --jobs {','.join(jobs)} > {loc}_info.json`"
            )

    # for copy_info in file_copies.values():
    #     print(
    #         f"src_path: {copy_info.src_path}\n"
    #         f"src_loc: {copy_info.src_location}\n"
    #         f"dst_path: {copy_info.dst_path}\n"
    #         f"dst_loc: {copy_info.dst_location}\n"
    #         f"transfer: {copy_info.end_time - copy_info.start_time}\n"
    #     )
    #     print("#" * 20)
    workflow = Workflow(workflow_start, workflow_end)
    workflow.steps.extend(
        sorted(steps.values(), key=lambda x: x.get_start()),
    )
    return workflow
