import argparse
import json
import os
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

from viewer.cli.schema import OutputConfig, StyleConfig
from viewer.core.utils import get_path


def create_cluster_info(args: argparse.Namespace) -> MutableMapping[str, Any]:
    clusters = {}
    if args.clusters_info:
        cluster_path = Path(args.clusters_info).resolve()
        with open(cluster_path) as f:
            yaml_data = YAML().load(f)
        for loc_name, path in yaml_data.items():
            if Path(path).is_absolute():
                abs_path = Path(path).resolve()
            else:
                abs_path = cluster_path.parent / path
            with open(abs_path) as f:
                # Assumption: supported only SLURM and
                # the files are all produced executing `sacct --json --jobs [JOB_ID]`
                for job in json.load(f)["jobs"]:
                    energy = None
                    for resource in job["tres"]["allocated"]:
                        if resource["type"] == "energy":
                            energy = resource["count"]
                    if energy is None:
                        print("Job", job["job_id"], "has no energy report")
                    elif float(energy) < 0:
                        raise Exception(f"Job {job['job_id']} has negative energy")
                    clusters.setdefault(loc_name, {})
                    clusters[loc_name][job["job_id"]] = {
                        "queue_starttime": job["time"]["submission"],
                        "queue_endtime": job["time"]["start"],
                        "avg_energy": float(energy) if energy else None,  # Joule
                    }
    return clusters


def create_output_config(args: argparse.Namespace) -> OutputConfig:
    return OutputConfig(
        outdir=get_path(args.outdir),
        filename=args.filename,
        extension=args.format,
    )


def create_style_config(args: argparse.Namespace) -> StyleConfig:
    config_data: MutableMapping[str, Any] = {}
    if args.style_config:
        if not os.path.exists(args.style_config):
            raise FileNotFoundError(args.style_config)
        with open(args.style_config) as f:
            yaml_data = YAML().load(f)
            config_data.update(yaml_data)

    cli_overrides = {
        "legend": args.legend,
        "color_palette": args.color_palette,
        "grouping_mode": args.grouping_mode,
        "xlim": args.xlim,
        "excluded_steps": args.excluded_steps,
    }
    config_data.update({k: v for k, v in cli_overrides.items() if v is not None})

    if args.color_map:
        # Expected format: "StepA:#FFF,StepB:#000" or passed multiple times
        current_map = config_data.get("color_map", {})
        for pair in args.color_map:
            if ":" in pair:
                key, val = pair.split(":", 1)
                current_map[key.strip()] = val.strip()
        config_data["color_map"] = current_map
    if args.renaming_steps:
        current_map = config_data.get("renaming_steps", {})
        for pair in args.renaming_steps:
            if ":" in pair:
                key, val = pair.split(":", 1)
                current_map[key.strip()] = val.strip()
        config_data["renaming_steps"] = current_map
    return StyleConfig.model_validate(config_data)
