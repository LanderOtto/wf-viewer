from __future__ import annotations

import glob
import json
import os.path
import re
from collections.abc import MutableMapping, MutableSequence
from datetime import timedelta

from viewer.core.entity import Step, Task
from viewer.core.utils import str_to_datetime

time_regex = r"\[[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+[0-9]{4}]"
job_version = r"New job version: "
new_job_version_regex = rf"{time_regex}.* {job_version}.+$"
scatter_regex = rf"{time_regex}.* Working on job 'CWLScatter'"


def _append_files_and_dirs(path, dirs, files):
    if os.path.isdir(path):
        dirs.append(path)
    elif os.path.isfile(path):
        files.append(path)
    else:
        raise FileNotFoundError(path)


def _iterative_get_files(dir_path: str) -> list[str]:
    files = []
    dirs = []
    _append_files_and_dirs(dir_path, dirs, files)
    while dirs:
        path = dirs.pop()
        for file in glob.glob(os.path.join(path, "*")):
            _append_files_and_dirs(file, dirs, files)
    return files


def get_standard_basename(name):
    return str(
        os.path.join(
            *(
                part
                for part in name.split(".")
                if not part.isdigit() and not part.startswith("_")
            )
        )
    )


def get_files(dir_path: str) -> list[str]:
    return _iterative_get_files(dir_path)


def get_key_from_value(
    elem: str, dictionary: MutableMapping[str, MutableSequence[str]]
) -> str | None:
    for key, values in dictionary.items():
        if elem in values:
            return key
    return None


def bottom_up(elem: str, dictionary: MutableMapping[str, MutableSequence[str]]) -> str:
    result = os.sep
    while (elem := get_key_from_value(elem, dictionary)) is not None:
        result = os.path.join(elem, result)
    return result


def translate_log(input_path: str):
    workflow_start = None
    workflow_end = None
    toil_jobs = {}
    for file in get_files(input_path):
        with open(file) as fd:
            data = json.load(fd)
        if "jobs" in data.keys():
            job_start = None
            if "log" in data.keys():
                time_search = re.search(time_regex, data["logs"]["messages"][0])
                job_start = str_to_datetime(
                    data["logs"]["messages"][0][
                        time_search.start() + 1 : time_search.end() - 1
                    ]
                )
                if workflow_start is None or job_start < workflow_start:
                    workflow_start = job_start
                time_search = re.search(time_regex, data["logs"]["messages"][-1])
                job_end = str_to_datetime(
                    data["logs"]["messages"][-1][
                        time_search.start() + 1 : time_search.end() - 1
                    ]
                )
                if workflow_end is None or job_end > workflow_end:
                    workflow_end = job_end

            for job in data["jobs"]:
                if len(parts := job["class_name"].split(" ")) == 2:
                    # identifier = get_standard_basename(parts[1])
                    identifier = parts[1]
                else:
                    # In case of CWLGather or CWLScatter, `identifier` is the cwl_type
                    identifier = parts[0]

                # Take children steps
                # for line in data["logs"]["messages"]:
                #     if identifier == "CWLScatter" and re.search(scatter_regex, line):
                #         *_, identifier, _ = line.split(" ")
                #     elif re.search(new_job_version_regex, line):
                #         job_version_search = re.search(job_version, line)
                #         cwl_type, *other = line[
                #             job_version_search.end() + 1 : -3
                #         ].split(" ")
                #         if cwl_type.strip("'") != "CWLGather" and identifier != (child_name := get_standard_basename(other[0])):
                #             if child_name in filesystem:
                #                 if identifier != filesystem[child_name].parent:
                #                     pass
                #                     # raise Exception(f"Step {child_name} has different parent nodes: {identifier} and {filesystem[child_name].parent}")
                #             else:
                #                 filesystem[child_name] = CWLStep(child_name, identifier or os.sep)

                # Take start and end times
                if parts[0] == "CWLJob" and job_start:
                    toil_jobs.setdefault(identifier, {"start_time": [], "end_time": []})
                    toil_jobs[identifier]["start_time"].append(job_start)
                    toil_jobs[identifier]["end_time"].append(job_end)
        else:
            print(f"WARN. File {file} has not the 'jobs' key. It has: {data.keys()}")
    print(workflow_end - workflow_start)

    steps = []
    for name, times in toil_jobs.items():
        steps.append(
            Step(
                name,
                [
                    Task(
                        start - workflow_start,
                        (
                            end - workflow_start
                            if start != end
                            else (end - workflow_start) + timedelta(milliseconds=100)
                        ),
                    )
                    for start, end in zip(times["start_time"], times["end_time"])
                ],
            )
        )
    return sorted(steps, key=lambda x: x.get_start()), workflow_start, workflow_end


# def analysis(input_path: str):
#     objects = {}
#     for file in get_files(input_path):
#         with open(file) as fd:
#             data = json.load(fd)
#         if "jobs" in data.keys():
#             for job in data["jobs"]:
#                 objects.setdefault(job["class_name"], []).append(
#                     data["logs"]["messages"]
#                 )
#         else:
#             print(f"WARN. File {file} has not the 'jobs' key. It has: {data.keys()}")
#     cwl_objects = {k: v for k, v in objects.items() if k.startswith("CWL")}
#
#     cwl_dictionary = {}
#     for key in cwl_objects.keys():
#         if len(elems := key.split(" ")) == 1:
#             cwl_obj, step_name = elems[0], None
#         elif len(elems) == 2:
#             cwl_obj, step_name = elems
#         else:
#             raise Exception(f"Key {key} has not valid name structure")
#         cwl_dictionary.setdefault(cwl_obj, []).append(step_name)
#
#     parts = {}
#     for name in cwl_dictionary["CWLWorkflow"] + cwl_dictionary["CWLJob"]:
#         prev = None
#         for part in name.split("."):
#             if not part.isdigit():
#                 parts.setdefault(part, [])
#                 if prev is not None and part not in parts[prev]:
#                     parts[prev].append(part)
#                 prev = part
#     print(json.dumps(parts, indent=2))
#
#     head = None
#     for part in parts.keys():
#         if all(part not in values for values in parts.values()):
#             if head is not None:
#                 raise Exception(
#                     f"There are two starting point of the workflow: {head} and {part}"
#                 )
#             head = part
#     if head is None:
#         raise Exception("There is no starting point in the workflow")
#
#
#     for toil_step_name, stats in  cwl_objects.items():
#         if len(elems := toil_step_name.split(" ")) == 2:
#             if elems[0] == "CWLJob":
