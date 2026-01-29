from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import plotly.io as pio

from viewer.cli.schema import GroupingMode, OutputConfig, StyleConfig
from viewer.core.entity import Workflow
from viewer.render.utils import save_file_log


def _create_dataframe(workflow: Workflow, grouping_mode: GroupingMode) -> pd.DataFrame:
    data = []
    match grouping_mode:
        case GroupingMode.AGGREGATE:
            for step in workflow.steps:
                data.append(
                    {
                        "Step": step.name,
                        "Start": workflow.start_date + step.get_start(),
                        "Finish": workflow.start_date + step.get_end(),
                        "NTasks": len(step.instances),
                    }
                )
        case GroupingMode.STEP | GroupingMode.TASK:
            for i, step in enumerate(workflow.steps):
                for j, job in enumerate(step.instances):
                    if job.end_time is None:
                        raise ValueError(
                            f"Job {i}-{j} in step {step.name} has no end time"
                        )
                    data.append(
                        {
                            "Step": step.name,
                            "Start": workflow.start_date + job.start_time,
                            "Finish": workflow.start_date + job.end_time,
                            "Task": f"{step.name}_{j}",
                        }
                    )
        case _:
            raise NotImplementedError(f"Unknown grouping mode: {grouping_mode}")
    return pd.DataFrame(data)


def _mathplotlib_rendering(df: pd.DataFrame, style: StyleConfig) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    start_ts = df["Start"].min()
    step_names = df["Step"].unique()
    colors = plt.colormaps[style.color_palette]
    step_color_map = {
        step: style.color_map.get(step, colors(i)) for i, step in enumerate(step_names)
    }

    # Horizontal bars
    for _, row in df.iterrows():
        duration = (row["Finish"] - row["Start"]).total_seconds()
        offset = (row["Start"] - start_ts).total_seconds()
        label_y = (
            row["Task"] if style.grouping_mode == GroupingMode.TASK else row["Step"]
        )
        ax.barh(
            label_y,
            duration,
            left=offset,
            height=0.5,
            color=step_color_map[row["Step"]],
        )
        if style.grouping_mode == GroupingMode.AGGREGATE:
            ax.text(
                offset + 1,
                label_y,
                row["NTasks"],
                ha="left",
                va="center",
                fontweight="bold",
                fontsize=14,
            )

    # Axis
    if style.xlim:
        ax.set_xlim(right=style.xlim)
    ax.set_xlabel("Time (seconds)", fontsize=18)
    ax.set_yticks([])
    plt.xticks(rotation=45, fontsize=18)
    ax.grid(True, which="both", axis="x", linestyle="--", alpha=0.5)

    # Legend
    if style.legend:
        handles = [
            plt.Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                markerfacecolor=step_color_map[s],
                markersize=10,
            )
            for s in step_names
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


def create_report(
    workflow: Workflow, out_config: OutputConfig, style_config: StyleConfig
) -> None:
    df = _create_dataframe(workflow, style_config.grouping_mode)
    if "html" in out_config.extension:
        fig = px.timeline(
            df,
            x_start="Start",
            x_end="Finish",
            y="Step",
            color="Step",
            text=(
                "NTasks"
                if style_config.grouping_mode == GroupingMode.AGGREGATE
                else None
            ),
        )
        fig.update_yaxes(visible=False)
        filepath = out_config.get_filepath("html")
        pio.write_html(fig, filepath)
        save_file_log(filepath, "report")

    if extensions := [e for e in out_config.extension if e != "html"]:
        _mathplotlib_rendering(df, style_config)
        for ext in extensions:
            filepath = out_config.get_filepath(ext)
            plt.tight_layout()
            plt.savefig(filepath)
            save_file_log(filepath, "report")
        plt.close()
