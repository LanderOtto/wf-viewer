import os
import re
from datetime import timedelta
from typing import MutableSequence

from viewer.core.entity import Step, Task
from viewer.core.utils import get_path, str_to_datetime

CWLTOOL_VERSIONS = [
    "3.1.20250110105449",
    "3.1.20251031082601",
]

# todo: Support different regex based on cwltool version
job_prefix = "[job "
job_regex = rf"\{job_prefix}.+]"
time_regex = r"\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}]"

end_regex = rf".*{time_regex}.*INFO.*{job_regex} completed success$"
path_regex = r"/([a-zA-Z0-9_-]+/?)?"
workflow_prefix = r"\[workflow .*]"

scatter_regex = r"_[0-9]+$"
start_regex = rf".*{time_regex}.*INFO.*{job_regex} {path_regex}.*$"
step_start_deploy = rf".*{time_regex}.*{workflow_prefix} starting step.*"
step_end_deploy = rf".*{time_regex}.*INFO.*{workflow_prefix} completed success$"
version_regex = r"cwltool [0-9]+.[0-9]+.[0-9]+"


def get_cwl_basename(name):
    if res := re.search(scatter_regex, name):
        return name[: res.start()]
    return name


def get_full_name(node, filesystem):
    if node.parent:
        parent = get_full_name(filesystem[node.parent], filesystem)
        return os.path.join(parent, get_cwl_basename(node.name))
    else:
        return get_cwl_basename(node.name)


class CWLStep:
    def __init__(self, name: str, parent: str | None):
        self.completed: bool = False
        self.name: str = name
        self.parent: str | None = parent


def scraping_log(input_paths: MutableSequence[str]):
    steps = []
    workflow_start_date = None
    workflow_end_date = None
    for input_path in input_paths:
        filesystem = {os.sep: CWLStep(os.sep, None)}
        workflow_name = os.sep
        # todo: define a config file and allow to associate for each path a workflow name
        # workflow_name = os.path.basename(
        #     os.path.dirname(input_path.replace("_", "-"))
        # )  # str(uuid.uuid4())
        # filesystem[workflow_name] = CWLStep(workflow_name, os.sep)
        step_start_dict = {}

        with open(get_path(input_path)) as fd:
            for line in fd:
                if workflow_start_date is None:
                    if time_search := re.search(time_regex, line):
                        workflow_start_date = str_to_datetime(
                            line[time_search.start() + 1 : time_search.end() - 1]
                        )
                        version_search = re.search(version_regex, line)
                        _, version = line[
                            version_search.start() : version_search.end()
                        ].split(" ")
                        if version not in CWLTOOL_VERSIONS:
                            raise Exception(
                                f"cwltool version {version} log not supported/tested"
                            )
                    else:
                        raise Exception("Execute cwltool with the flag `--timestamps`")
                elif re.match(step_start_deploy, line):
                    parent_match = re.search(workflow_prefix, line)
                    child_match = re.search("starting step .*$", line)
                    parent_step = line[
                        parent_match.start()
                        + len("[workflow ") : parent_match.end()
                        - 1
                    ]
                    child_step = line[
                        child_match.start() + len("starting step ") : child_match.end()
                    ]
                    if parent_step not in filesystem:
                        parent_step = get_cwl_basename(parent_step)
                    if child_step in filesystem:
                        raise Exception(f"Node {child_step} already in filesystem")
                    filesystem[child_step] = CWLStep(
                        child_step, parent_step or workflow_name
                    )
                elif re.match(step_end_deploy, line):
                    # parent_step = re.search(workflow_prefix, line)
                    # parent_step = line[parent_step.start() + len("[workflow "): parent_step.end() - 1]
                    pass
                elif re.match(start_regex, line):
                    time_match = re.search(time_regex, line)
                    start_time = line[time_match.start() + 1 : time_match.end() - 1]
                    job_match = re.search(job_regex, line)
                    job_name = line[
                        job_match.start() + len(job_prefix) : job_match.end() - 1
                    ]
                    step_start_dict[job_name] = [
                        str_to_datetime(start_time) - workflow_start_date
                    ]
                elif re.match(end_regex, line):
                    time_match = re.search(time_regex, line)
                    end_time = line[time_match.start() + 1 : time_match.end() - 1]
                    workflow_end_date = str_to_datetime(end_time)
                    job_match = re.search(job_regex, line)
                    job_name = line[
                        job_match.start() + len(job_prefix) : job_match.end() - 1
                    ]
                    step_start_dict[job_name].append(
                        workflow_end_date - workflow_start_date
                    )
        step_group_by = {}
        for job_name, (start_time, end_time) in step_start_dict.items():
            step_name = get_full_name(filesystem[job_name], filesystem)
            step_group_by.setdefault(step_name, []).append((start_time, end_time))
        for step_name, times in step_group_by.items():
            steps.append(
                Step(
                    step_name,
                    [
                        Task(
                            start_time,
                            (
                                end_time
                                if start_time != end_time
                                else end_time + timedelta(milliseconds=100)
                            ),
                        )
                        for start_time, end_time in times
                    ],
                )
            )
    return (
        sorted(steps, key=lambda x: x.get_start()),
        workflow_start_date,
        workflow_end_date,
    )
