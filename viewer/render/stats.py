import json
import statistics
from collections.abc import MutableMapping, MutableSequence
from datetime import timedelta
from typing import Any

import numpy as np

from viewer.cli.schema import OutputConfig
from viewer.core.entity import Step, Workflow
from viewer.render.utils import save_file_log


def _show_task_distributions(
    task_times: MutableSequence[float],
    num_bins: int = 10,
    bin_spacing: str = "normal",
) -> None:
    if not task_times:
        print("No task durations found in the JSON.")
        return

    all_times = np.array(task_times)
    # all_times = all_times[all_times < 90]

    # Calculate Bins (e.g., 6 equal-width intervals)
    min_t, max_t = all_times.min(), all_times.max()
    if bin_spacing == "normal":
        # Normal
        edges = np.linspace(min_t, max_t, num_bins + 1)
    elif bin_spacing == "quantile":
        # Quantile
        edges = np.quantile(all_times, np.linspace(0, 1, num_bins + 1))
        edges = np.unique(edges)
    elif bin_spacing == "log":
        # Log
        offset = 1e-6 if min_t == 0 else 0
        log_min = np.log10(min_t + offset)
        log_max = np.log10(max_t)

        edges = np.logspace(log_min, log_max, num_bins + 1)
    else:
        raise ValueError(f"Unknown value for bin_spacing: {bin_spacing}")
    # FORCE the first and last edges to encompass the actual data exactly
    edges[0] = min_t
    edges[-1] = max_t
    # Get counts for each range
    counts, bin_edges = np.histogram(all_times, bins=edges)
    # Print distribution
    print(
        f"--- Distribution of {len(all_times)} tasks (Range: {min_t:.2f}s to {max_t:.2f}s) ---"
    )
    n_digits = len(str(counts.max()))
    acc = 0
    for i in range(len(counts)):
        print(
            f"{counts[i]:>{n_digits}} tasks in range {bin_edges[i]:.2f}s to {bin_edges[i + 1]:.2f}s"
        )
        acc += counts[i]
    print(f"{acc} / {len(all_times)} tasks processed")


def get_step_metrics(step: Step) -> MutableMapping[str, Any]:
    """Calculates all metrics for a step and returns them as a dictionary."""
    durations = [task.get_duration() for task in step.instances if task.get_duration()]
    duration_total = step.get_duration()

    metrics: MutableMapping[str, Any] = {
        "name": step.name,
        "n_of_tasks": len(step.instances),
        "total_exec_seconds": duration_total.total_seconds(),
        "instance_metrics": None,
    }

    if len(step.instances) > 1:
        instance_starts = [inst.start_time for inst in step.instances]
        deploy_time = max(instance_starts) - min(instance_starts)
        queue_times = [
            q.get_duration().total_seconds()
            for task in step.instances
            for q in task.queue_times
        ]

        metrics["instance_metrics"] = {
            "deploy_time_seconds": deploy_time.total_seconds(),
            "executions": {
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
            },
            "queues": {
                "min_seconds": (min(queue_times) if queue_times else 0),
                "max_seconds": (max(queue_times) if queue_times else 0),
                "avg_seconds": (statistics.mean(queue_times) if queue_times else 0),
            },
        }
    else:
        queue_time = next(
            (
                q.get_duration().total_seconds()
                for task in step.instances
                for q in task.queue_times
            ),
            None,
        )
        metrics["queue_time_seconds"] = queue_time

    return metrics


def print_terminal_report(data: dict[str, Any]):
    """Prints a clean, formatted report to the terminal."""
    for step in data["steps"]:
        print(f"\n{'#' * 40}")
        print(f"Step:\t\t\t{step['name']}")
        print(f"Tasks:\t\t\t{step['n_of_tasks']}")
        print(f"Total Exec:\t\t{step['total_exec_seconds']:.4f}s")
        if metrics := step["instance_metrics"]:
            print(f"Deploy Time:\t{metrics['deploy_time_seconds']:.4f}s")
            print(
                f"Execution times:\n\tRange [m/M]:\t{metrics['executions']['min_seconds']:.4f}s / {metrics['executions']['max_seconds']:.4f}s"
            )
            print(f"\tAverage:\t\t{metrics['executions']['avg_seconds']:.4f}s")
            print(
                f"Queue times:\n\tRange [m/M]:\t{metrics['queues']['min_seconds']:.4f}s / {metrics['queues']['max_seconds']:.4f}s"
            )
            print(f"\tAverage:\t\t{metrics['queues']['avg_seconds']:.4f}s")
        elif step["queue_time_seconds"] is not None:
            print(f"Queue time:\t\t{step['queue_time_seconds']:.4f}s")

    print(f"\n{'=' * 40}")
    print("WORKFLOW SUMMARY")
    print(f"Total Steps:    {data['workflow']['total_tasks']}")
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
        total_tasks = sum(s["n_of_tasks"] for s in steps_data)
        duration = (
            workflow.end_date - workflow.start_date
            if workflow.end_date
            else timedelta(0)
        )

        report_data = {
            "workflow": {
                "total_tasks": total_tasks,
                "start": str(workflow.start_date),
                "end": str(workflow.end_date),
                "duration_seconds": duration.total_seconds(),
            },
            "steps": steps_data,
        }

        if show_stats:
            print_terminal_report(report_data)

            task_times = []
            for s in workflow.steps:
                for t in s.instances:
                    task_times.append((t.end_time - t.start_time).total_seconds())
            _show_task_distributions(task_times)

        if save_stats:
            stats_path = out_config.get_statspath()
            with open(stats_path, "w") as f:
                json.dump(report_data, f, indent=4)
            save_file_log(stats_path, "stats")
