import argparse
import os
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

from viewer.cli.schema import (
    LocationType,
    OutputConfig,
    StyleConfig,
    load_location_config,
)
from viewer.core.entity import Location, SlurmLocation
from viewer.core.utils import get_path


def get_location_class(loc_type: LocationType):
    match loc_type:
        case LocationType.SLURM:
            return SlurmLocation
        case _:
            raise NotImplementedError(f"Location type {loc_type} not yet implemented.")


def create_location(args) -> MutableMapping[str, Location]:
    if not args.locations:
        return {}

    config_path = Path(args.locations).resolve()
    location_config = load_location_config(str(config_path))

    locations = {}
    for name, loc_config in location_config.locations.items():
        loc_class = get_location_class(loc_config.type)
        instance = loc_class(name, loc_config)

        instance.parse(config_path.parent)
        locations[name] = instance
    return locations


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
