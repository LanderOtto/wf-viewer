import os
import re
from collections.abc import MutableSequence
from datetime import timedelta

from viewer.core.entity import Step, Task
from viewer.core.utils import get_path, str_to_datetime

time_regex = r"\[(?P<timestamp>[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})]"
job_regex = r"\[job (?P<job>[^\]]+)\]"
workflow_regex = r"\[workflow\s*(?P<workflow>[^\]]*)\s*\]"
start_regex = rf"^{time_regex}.*INFO.*{job_regex}.*$"
end_regex = rf"^{time_regex}.*INFO.*{job_regex} completed success$"
step_start_deploy = rf"^{time_regex}.*{workflow_regex} starting step (?P<child>.*)$"

scatter_regex = r"_[0-9]+$"


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


def strip_ansi_codes(text):
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)


def translate_log(input_paths: MutableSequence[str]):
    steps = []
    workflow_start_date = None
    workflow_end_date = None

    for input_path in input_paths:
        filesystem = {os.sep: CWLStep(os.sep, None)}
        workflow_name = os.sep
        step_start_dict = {}

        with open(get_path(input_path)) as fd:
            for line in fd:
                line = strip_ansi_codes(line.strip())

                if workflow_start_date is None:
                    if m := re.match(rf"{time_regex}.*", line):
                        workflow_start_date = str_to_datetime(m.group("timestamp"))
                        # current_version = m.group('version')
                    continue

                if m := re.match(step_start_deploy, line):
                    parent_step = m.group("workflow") or os.sep
                    child_step = m.group("child").strip()

                    if parent_step not in filesystem:
                        parent_step = get_cwl_basename(parent_step)

                    if child_step not in filesystem:
                        filesystem[child_step] = CWLStep(
                            child_step, parent_step or workflow_name
                        )

                elif m := re.match(end_regex, line):
                    end_time = str_to_datetime(m.group("timestamp"))
                    job_name = m.group("job")

                    if job_name in step_start_dict:
                        step_start_dict[job_name].append(end_time - workflow_start_date)
                        workflow_end_date = end_time

                elif m := re.match(start_regex, line):
                    start_time = str_to_datetime(m.group("timestamp"))
                    job_name = m.group("job")
                    if job_name not in filesystem:
                        filesystem[job_name] = CWLStep(job_name, workflow_name)
                    step_start_dict[job_name] = [start_time - workflow_start_date]

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
