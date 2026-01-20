from __future__ import annotations

import base64
from collections.abc import MutableMapping, MutableSequence
from datetime import datetime
from typing import Any

import numpy as np

from viewer.core.entity import Step, Task
from viewer.core.utils import str_to_datetime


def get_x(elem):
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


def check_and_analysis(data: MutableMapping[str, Any]) -> tuple[datetime, datetime]:
    workflow_start_date, workflow_end_date = None, None
    for elem in data["data"]:
        # elem["x"] is expressed in milliseconds
        new_elem_x = get_x(elem)
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


def get_steps(
    data: MutableMapping[str, Any], workflow_start_date: datetime
) -> MutableSequence[Step]:
    steps = []
    for elem in data["data"]:
        instances = []
        new_elem_x = get_x(elem)
        for start_date_str, exec_time in zip(elem["base"], new_elem_x):
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
        steps.append(Step(elem["name"], instances))
    return steps
