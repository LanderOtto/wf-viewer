import json
import statistics
from collections.abc import MutableMapping
from datetime import timedelta
from typing import Any

from viewer.cli.schema import OutputConfig
from viewer.core.entity import Step, Workflow
from viewer.render.utils import save_file_log


def get_step_metrics(step: Step) -> MutableMapping[str, Any]:
    """Calculates all metrics for a step and returns them as a dictionary."""
    durations = [inst.get_duration() for inst in step.instances if inst.get_duration()]
    duration_total = step.get_duration()

    metrics = {
        "name": step.name,
        "instances_count": len(step.instances),
        "total_exec_seconds": duration_total.total_seconds(),
        "instance_metrics": None,
    }

    if len(step.instances) > 1:
        instance_starts = [inst.start_time for inst in step.instances]
        deploy_time = max(instance_starts) - min(instance_starts)

        metrics["instance_metrics"] = {
            "deploy_time_seconds": deploy_time.total_seconds(),
            "min_seconds": (
                min(d.total_seconds() for d in durations) if durations else 0
            ),
            "max_seconds": (
                max(d.total_seconds() for d in durations) if durations else 0
            ),
            "avg_seconds": (
                statistics.mean(d.total_seconds() for d in durations)
                if durations
                else 0
            ),
        }
    return metrics


def print_terminal_report(data: dict[str, Any]):
    """Prints a clean, formatted report to the terminal."""
    for step in data["steps"]:
        print(f"\n{'#' * 40}")
        print(f"Step:           {step['name']}")
        print(f"Instances:      {step['instances_count']}")
        print(f"Total Exec:     {step['total_exec_seconds']:.4f}s")

        m = step["instance_metrics"]
        if m:
            print(f"Deploy Time:    {m['deploy_time_seconds']:.4f}s")
            print(f"Range [m/M]:    {m['min_seconds']:.4f}s / {m['max_seconds']:.4f}s")
            print(f"Average:        {m['avg_seconds']:.4f}s")

    print(f"\n{'=' * 40}")
    print("WORKFLOW SUMMARY")
    print(f"Total Steps:    {data['workflow']['total_instances']}")
    print(f"Start:          {data['workflow']['start']}")
    print(f"End:            {data['workflow']['end']}")
    print(f"Total Duration: {data['workflow']['duration_seconds']:.4f}s")
    print(f"{'=' * 40}\n")


def create_stats(
    workflow: Workflow,
    out_config: OutputConfig,
    show_stats: bool,
    save_stats: bool,
) -> None:
    if show_stats or save_stats:
        steps_data = [
            get_step_metrics(s) for s in sorted(workflow.steps, key=lambda s: s.name)
        ]
        total_instances = sum(s["instances_count"] for s in steps_data)
        duration = (
            workflow.end_date - workflow.start_date
            if workflow.end_date
            else timedelta(0)
        )

        report_data = {
            "workflow": {
                "total_instances": total_instances,
                "start": str(workflow.start_date),
                "end": str(workflow.end_date),
                "duration_seconds": duration.total_seconds(),
            },
            "steps": steps_data,
        }

        if show_stats:
            print_terminal_report(report_data)

        if save_stats:
            stats_path = out_config.get_statspath()
            with open(stats_path, "w") as f:
                json.dump(report_data, f, indent=4)
            save_file_log(stats_path, "stats")
