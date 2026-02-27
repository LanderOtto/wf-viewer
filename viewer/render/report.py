from __future__ import annotations

import matplotlib.patheffects as path_effects
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
                locs = step.get_locations()
                data.append(
                    {
                        "Step": step.name,
                        "Start": workflow.start_date + step.get_start(),
                        "Finish": workflow.start_date + step.get_end(),
                        "NTasks": len(step.instances),
                        "Energy": step.get_energy(),
                        "Duration": step.get_duration(),
                        "Locations": ",".join(locs) if locs else None,
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
                            "Energy": job.get_energy(),
                            "Duration": job.get_duration(),
                            "Location": job.get_location(),
                        }
                    )
        case _:
            raise NotImplementedError(f"Unknown grouping mode: {grouping_mode}")
    return pd.DataFrame(data)


def _format_energy(joules: float | None) -> str:
    if joules is None:
        return "NaN Wh"
    elif joules == 0:
        return "0.0 Wh"
    elif joules < 0:
        raise ValueError("Energy value cannot be negative")

    # 1 Wh = 3600 J
    watt_hours = joules / 3600

    units = [
        ("GWh", 1e9),
        ("MWh", 1e6),
        ("kWh", 1e3),
        ("Wh", 1),
        ("mWh", 1e-3),
    ]

    for unit_name, factor in units:
        value = watt_hours / factor
        if value >= 1:
            return f"{value:.3f} {unit_name}"

    # If extremely small (less than 1 mWh)
    return f"{watt_hours / 1e-3:.3f} mWh"


def _rendering_energy(df: pd.DataFrame, style: StyleConfig) -> None:
    fig, ax = plt.subplots(figsize=(12, 7))
    start_ts = df["Start"].min()

    is_aggregate = style.grouping_mode == GroupingMode.AGGREGATE
    color_key = "Locations" if is_aggregate else "Location"

    unique_locs = df[color_key].unique()
    colors = plt.colormaps[style.color_palette]
    loc_color_map = {
        loc: colors(i / max(1, len(unique_locs) - 1))
        for i, loc in enumerate(unique_locs)
    }

    for _, row in df.iterrows():
        duration = (row["Finish"] - row["Start"]).total_seconds()
        offset = (row["Start"] - start_ts).total_seconds()

        label_y = row["Step"] if is_aggregate else row["Task"]
        ax.barh(
            label_y,
            duration,
            left=offset,
            height=0.6,
            color=loc_color_map[row[color_key]],
            edgecolor="black",
            alpha=0.8,
        )
        energy_label = _format_energy(row["Energy"])
        if is_aggregate:
            label_text = f"{energy_label} ({row['NTasks']}T)"
        else:
            label_text = energy_label
        text_x = offset + (duration / 2)
        txt = ax.text(
            text_x,
            label_y,
            label_text,
            ha="center",
            va="center",
            fontsize=10,
            fontweight="bold",
            color="white",
        )
        txt.set_path_effects([path_effects.withStroke(linewidth=2, foreground="black")])

    # Styling
    ax.set_xlabel("Time (seconds)", fontsize=14)
    ax.set_title("Workflow Timeline", fontsize=16)

    if style.xlim:
        ax.set_xlim(0, style.xlim)
    ax.grid(True, axis="x", linestyle="--", alpha=0.3)

    # Legend
    if style.legend:
        handles = [
            plt.Rectangle((0, 0), 1, 1, color=loc_color_map[loc]) for loc in unique_locs
        ]
        ax.legend(
            handles,
            unique_locs,
            title="Locations",
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
        )
    plt.tight_layout()


def _rendering_time(df: pd.DataFrame, style: StyleConfig) -> None:
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
        _rendering_time(df, style_config)
        for ext in extensions:
            filepath = out_config.get_filepath(ext)
            plt.tight_layout()
            plt.savefig(filepath)
            save_file_log(filepath, "report")
        plt.close()

    if any(row["Energy"] is not None for _, row in df.iterrows()):
        if extensions := [e for e in out_config.extension]:
            _rendering_energy(df, style_config)
            for ext in extensions:
                if ext == "html":
                    print("WARNING: Format HTML does not available for energy plot")
                    continue
                filepath = out_config.get_filepath(ext, postfix=".energy")
                plt.tight_layout()
                plt.savefig(filepath)
                save_file_log(filepath, "report")
            plt.close()
    else:
        print("WARNING: Workflow steps do not have energy information")
