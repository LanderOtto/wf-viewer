from __future__ import annotations

import os
import statistics
import sys
from datetime import datetime, timedelta
from typing import MutableSequence

import pandas as pd
import plotly.express as px
import plotly.io as pio

from viewer.core.entity import Step
from viewer.render.utils import multi_print, print_split_section


def plot_gantt(
    steps: MutableSequence[Step],
    workflow_start_date: datetime,
    outdir: str,
    filename: str,
    format: str,
) -> None:
    group_by_step = False # todo: add param
    if group_by_step:
        df = pd.DataFrame(
            [
                dict(
                    Task=step.name,
                    Start=workflow_start_date + step.get_start(),
                    Finish=workflow_start_date + step.get_end(),
                    Jobs=len(step.instances),
                )
                for step in steps
            ]
        )
        fig = px.timeline(
            df, x_start="Start", x_end="Finish", y="Task", text="Jobs", color="Task"
        )
    else:
        df = pd.DataFrame(
            [
                dict(
                    Task=step.name,
                    Start=workflow_start_date + job.start,
                    Finish=workflow_start_date + job.end,
                    Jobs="",
                )
                for step in steps
                for job in step.instances
            ]
        )
        fig = px.timeline(
            df, x_start="Start", x_end="Finish", y="Task", text="Jobs", color="Task"
        )
    fig.update_yaxes(visible=False)
    _, ext = os.path.splitext(filename)
    output_filepath = os.path.join(
        outdir, filename if f".{format}" == ext else f"{filename}.{format}"
    )
    pio.write_html(fig, output_filepath)
    print(f"Created file {output_filepath}")


def _default_format(
    steps, workflow_start_date, workflow_end_date, file_descriptors
) -> None:
    n_of_step_instances = 0
    for step in sorted(steps, key=lambda s: s.name):
        multi_print(f"Step name:      {step.name}", file_descriptors=file_descriptors)
        multi_print(
            f"N.of instances: {len(step.instances)}", file_descriptors=file_descriptors
        )
        n_of_step_instances += len(step.instances)
        multi_print(
            f"Start time:     {step.get_start()}", file_descriptors=file_descriptors
        )
        multi_print(
            f"End time:       {step.get_end()}", file_descriptors=file_descriptors
        )
        step_exec = step.get_exec()
        str_step_exec_seconds = (
            f"{step_exec.total_seconds():.4f}" if step_exec is not None else None
        )
        multi_print(
            f"Exec time:      {step_exec} = {str_step_exec_seconds} seconds",
            file_descriptors=file_descriptors,
        )
        if len(step.instances) > 1:
            # tempo che intercorre tra il deploy della prima istanza e l'ultima
            first_instance_deploy = min(instance.start for instance in step.instances)
            last_instance_deploy = max(instance.start for instance in step.instances)
            instance_deploy_time = last_instance_deploy - first_instance_deploy
            multi_print(
                f"Instance deploy time:   {instance_deploy_time} = {instance_deploy_time.total_seconds():.4f} seconds",
                file_descriptors=file_descriptors,
            )
            instance_exec = [
                instance.get_exec()
                for instance in step.instances
                if instance.get_exec() is not None
            ]
            min_instance_exec = min(instance_exec) if instance_exec else None
            str_min_instance_seconds = (
                f"{min_instance_exec.total_seconds():.4f}"
                if min_instance_exec
                else None
            )
            multi_print(
                f"Min instance exec:      {min_instance_exec} = {str_min_instance_seconds} seconds",
                file_descriptors=file_descriptors,
            )
            max_instance_exec = max(instance_exec) if instance_exec else None
            str_max_instance_seconds = (
                f"{max_instance_exec.total_seconds():.4f}"
                if max_instance_exec
                else None
            )
            multi_print(
                f"Max instance exec:      {max_instance_exec} = {str_max_instance_seconds} seconds",
                file_descriptors=file_descriptors,
            )
            avg_instance_exec = (
                timedelta(
                    seconds=statistics.mean(
                        instance.total_seconds() for instance in instance_exec
                    )
                )
                if instance_exec
                else None
            )
            str_avg_instance_seconds = (
                f"{avg_instance_exec.total_seconds():.4f}"
                if avg_instance_exec
                else None
            )
            multi_print(
                f"Avg instances exec:     {avg_instance_exec} = {str_avg_instance_seconds} seconds",
                file_descriptors=file_descriptors,
            )
        print_split_section(file_descriptors=file_descriptors)
    multi_print(
        "workflow total number of step instances: ",
        n_of_step_instances,
        file_descriptors=file_descriptors,
    )
    multi_print(
        "workflow start date: ", workflow_start_date, file_descriptors=file_descriptors
    )
    multi_print(
        "workflow end date:   ", workflow_end_date, file_descriptors=file_descriptors
    )
    multi_print(
        "workflow exec time:  ",
        workflow_end_date - workflow_start_date if workflow_end_date else None,
        file_descriptors=file_descriptors,
    )
    multi_print(file_descriptors=file_descriptors)


def show_analysis(
    steps: MutableSequence[Step],
    workflow_start_date: datetime,
    workflow_end_date: datetime,
    file_stats: str | None,
    quiet: bool,
    json_stats: bool,
) -> None:
    if json_stats:
        raise NotImplementedError()
    else:
        file_descriptors = []
        if file_stats:
            file_descriptor = open(file_stats, "w")
            file_descriptors.append(file_descriptor)
        else:
            file_descriptor = None
        if not quiet:
            file_descriptors.append(sys.stdout)
        try:
            _default_format(
                steps, workflow_start_date, workflow_end_date, file_descriptors
            )
        finally:
            if file_descriptor:
                multi_print(
                    "Created file", file_stats, file_descriptors=file_descriptors
                )
                file_descriptor.close()
