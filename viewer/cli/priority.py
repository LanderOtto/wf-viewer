import argparse
import os
from collections.abc import MutableMapping
from typing import Any

from ruamel.yaml import YAML

from viewer.cli.schema import OutputConfig, StyleConfig
from viewer.core.utils import get_path


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
    return StyleConfig.model_validate(config_data)


def create_output_config(args: argparse.Namespace) -> OutputConfig:
    return OutputConfig(
        outdir=get_path(args.outdir),
        filename=args.filename,
        extension=args.format,
    )
