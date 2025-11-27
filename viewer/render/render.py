from __future__ import annotations

import os
import statistics
import sys
from datetime import datetime, timedelta
from typing import MutableSequence

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import plotly.io as pio

from viewer.core.entity import Step
from viewer.render.utils import multi_print, print_split_section


def plot_barh(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    start = min(row["Start"] for _, row in df.iterrows())
    step_names = df["Step"].unique()
    # colors = plt.cm.get_cmap("tab20", len(step_names))
    colors = plt.cm.get_cmap("Accent", len(step_names))
    step_color_map = {step: colors(i) for i, step in enumerate(step_names)}
    for k, vs in step_color_map.items():
        print(k, [v * 255 for v in vs])
    for _, row in df.iterrows():
        ax.barh(
            row["Task"],
            (row["Finish"] - row["Start"]).total_seconds(),
            left=(row["Start"] - start).total_seconds(),
            height=0.5,
            color=step_color_map[row["Step"]],
        )

    # ax.set_xlim(right=250)
    ax.set_xlabel("Time (seconds)", fontsize=18)
    ax.set_yticks([])
    handles = [
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor=step_color_map[step],
            markersize=10,
        )
        for step in step_names
    ]
    ax.legend(
        handles,
        step_names,
        title="Steps",
        loc="lower right",
        fontsize=18,
        title_fontsize=20,
        bbox_to_anchor=(1, 0),
        frameon=False,
    )
    plt.xticks(rotation=45, fontsize=18)
    ax.grid(True, which="both", axis="x", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.show()


def plot_gantt(
    steps: MutableSequence[Step],
    workflow_start_date: datetime,
    outdir: str,
    filename: str,
    format: str,
) -> None:
    streamflow_style = True  # todo: add param
    if streamflow_style:
        group_by_step = True
        i = 0
        for step in steps:
            for job in step.instances:
                job.name = str(i)
                i += 1
        df = pd.DataFrame(
            [
                dict(
                    Step=step.name,
                    Start=workflow_start_date + job.start,
                    Finish=workflow_start_date + job.end,
                    Task=job.name,
                )
                for step in steps
                for job in step.instances
            ]
        )
        fig = px.timeline(
            df,
            x_start="Start",
            x_end="Finish",
            y="Step" if group_by_step else "Task",
            color="Step",
        )
    else:
        df = pd.DataFrame(
            [
                dict(
                    Step=step.name,
                    Start=workflow_start_date + step.get_start(),
                    Finish=workflow_start_date + step.get_end(),
                    Tasks=len(step.instances),
                )
                for step in steps
            ]
        )
        fig = px.timeline(
            df, x_start="Start", x_end="Finish", y="Step", text="Tasks", color="Step"
        )
    fig.update_yaxes(visible=False)
    _, ext = os.path.splitext(filename)
    output_filepath = os.path.join(
        outdir, filename if f".{format}" == ext else f"{filename}.{format}"
    )
    pio.write_html(fig, output_filepath)
    plot_barh(df)
    # format_="png"
    # pio.write_image(fig, format=format_, file=os.path.join(
    #     outdir, filename if f".{format_}" == ext else f"{filename}.{format_}"
    # ))
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
