import statistics
import sys
from collections.abc import MutableSequence
from datetime import datetime, timedelta

from viewer.core.entity import Step
from viewer.render.utils import format_seconds, multi_print, print_split_section


def _print_analysis_report(steps, start_date, end_date, file_descriptors):
    """Orchestrates the printing of the full analysis report."""
    total_instances = 0
    for step in sorted(steps, key=lambda s: s.name):
        print_step_details(step, file_descriptors)
        total_instances += len(step.instances)
        print_split_section(file_descriptors=file_descriptors)

    multi_print(
        f"Workflow total instances: {total_instances}",
        file_descriptors=file_descriptors,
    )
    multi_print(
        f"Workflow start:           {start_date}", file_descriptors=file_descriptors
    )
    multi_print(
        f"Workflow end:             {end_date}", file_descriptors=file_descriptors
    )
    if end_date and start_date:
        multi_print(
            f"Total duration:           {end_date - start_date}",
            file_descriptors=file_descriptors,
        )


def print_step_details(step: Step, file_descriptors: list):
    """Calculates and prints metrics for a single workflow step."""
    multi_print(f"Step name:      {step.name}", file_descriptors=file_descriptors)
    multi_print(
        f"N.of instances: {len(step.instances)}", file_descriptors=file_descriptors
    )

    duration = step.get_duration()
    multi_print(
        f"Exec time:      {duration} = {format_seconds(duration)} seconds",
        file_descriptors=file_descriptors,
    )

    if len(step.instances) > 1:
        instance_starts = [inst.start_time for inst in step.instances]
        deploy_time = max(instance_starts) - min(instance_starts)
        multi_print(
            f"Instance deploy time: {deploy_time} = {format_seconds(deploy_time)} seconds",
            file_descriptors=file_descriptors,
        )

        durations = [i.get_duration() for i in step.instances if i.get_duration()]
        if durations:
            avg_dur = timedelta(
                seconds=statistics.mean(d.total_seconds() for d in durations)
            )
            multi_print(
                f"Min instance exec:  {format_seconds(min(durations))}s",
                file_descriptors=file_descriptors,
            )
            multi_print(
                f"Max instance exec:  {format_seconds(max(durations))}s",
                file_descriptors=file_descriptors,
            )
            multi_print(
                f"Avg instances exec: {format_seconds(avg_dur)}s",
                file_descriptors=file_descriptors,
            )


def create_stats(
    steps: MutableSequence[Step],
    workflow_start_date: datetime,
    workflow_end_date: datetime,
    file_stats: str | None,
    quiet: bool,
    json_stats: bool,
) -> None:
    """Main entry point for textual analysis."""
    if json_stats:
        raise NotImplementedError("JSON stats support is not yet implemented.")

    file_descriptors = []
    f_handle = None

    if file_stats:
        f_handle = open(file_stats, "w")
        file_descriptors.append(f_handle)
    if not quiet:
        file_descriptors.append(sys.stdout)

    try:
        _print_analysis_report(
            steps, workflow_start_date, workflow_end_date, file_descriptors
        )
    finally:
        if f_handle:
            multi_print(f"Created file {file_stats}", file_descriptors=file_descriptors)
            f_handle.close()
