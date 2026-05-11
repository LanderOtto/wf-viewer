from __future__ import annotations

import base64
import json
import os
from collections.abc import MutableMapping, MutableSequence
from datetime import datetime
from typing import Any

import numpy as np

from viewer.core.entity import Action, Location, LocationJobStats, Step, Task, Workflow
from viewer.core.utils import str_to_datetime


def _get_elem_x(elem: MutableMapping[str, Any]) -> np.ndarray:
    new_elem_x = []
    if isinstance(elem["x"], list):
        for x in elem["x"]:
            if isinstance(x, dict):
                new_elem_x.extend(
                    np.frombuffer(base64.b64decode(x["bdata"]), dtype=x["dtype"])
                )
        if len(new_elem_x) == 0:
            new_elem_x = elem["x"]
    elif isinstance(elem["x"], dict):
        new_elem_x = np.frombuffer(
            base64.b64decode(elem["x"]["bdata"]), dtype=elem["x"]["dtype"]
        )
    return new_elem_x


def _extract_dates(data: MutableMapping[str, Any]) -> tuple[datetime, datetime]:
    workflow_start_date, workflow_end_date = None, None
    for elem in data["data"]:
        # elem["x"] is expressed in milliseconds
        new_elem_x = _get_elem_x(elem)
        for start_date_str, exec_time in zip(elem["base"], new_elem_x):
            if not (curr_start := str_to_datetime(start_date_str)):
                raise Exception(f"Step {elem['name']} does not have a start date")
            curr_end = datetime.fromtimestamp(
                datetime.timestamp(curr_start) + exec_time / 1000
            )
            if workflow_start_date is None or curr_start < workflow_start_date:
                workflow_start_date = curr_start
            if workflow_end_date is None or curr_end > workflow_end_date:
                workflow_end_date = curr_end
    if workflow_start_date is None:
        raise Exception("Impossible find start date of workflow")
    if workflow_end_date is None:
        raise Exception("Impossible find end date of workflow")
    return workflow_start_date, workflow_end_date


def _step_location(
    location_metadata: MutableSequence[Location],
) -> MutableMapping[str, MutableSequence[LocationJobStats]]:
    step_location = {}
    for loc in location_metadata:
        for job_stat in loc.jobs.values():
            step_location.setdefault(
                os.path.dirname(job_stat.streamflow_task_name), []
            ).append(job_stat)
    step_location = {
        k: sorted(
            jobs,
            key=lambda j: int(os.path.basename(j.streamflow_task_name).split(".")[-1]),
        )
        for k, jobs in step_location.items()
    }
    return step_location


def _get_steps(
    data: MutableMapping[str, Any],
    workflow_start_date: datetime,
    location_metadata: MutableMapping[str, Location],
) -> MutableSequence[Step]:
    steps = []
    loc_stats = _step_location(location_metadata.values())
    for elem in data["data"]:
        instances: MutableSequence[Task] = []
        new_elem_x = _get_elem_x(elem)
        for i, (start_date_str, exec_time) in enumerate(
            zip(elem["base"], new_elem_x, strict=True)
        ):
            instance_start_date = str_to_datetime(start_date_str)
            instance_end_date = datetime.fromtimestamp(
                datetime.timestamp(instance_start_date) + exec_time / 1000
            )
            instances.append(
                Task(
                    instance_start_date - workflow_start_date,
                    instance_end_date - workflow_start_date,
                )
            )
            if elem["name"] in loc_stats.keys():
                step_stat = loc_stats[elem["name"]][i]
                instances[-1].queue_times.append(
                    Action(
                        start_time=step_stat.submit_time, end_time=step_stat.start_time
                    )
                )
        steps.append(Step(elem["name"], instances))
    return steps


def translate_report(
    input_path: str, location_metadata: MutableMapping[str, Any]
) -> Workflow:
    with open(input_path) as fd:
        sf_report = json.load(fd)
    start_date, end_date = _extract_dates(sf_report)
    steps = _get_steps(sf_report, start_date, location_metadata)
    workflow = Workflow(start_date, end_date)
    workflow.steps.extend(steps)
    return workflow
