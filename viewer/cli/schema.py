from __future__ import annotations

import os
import sys
from collections.abc import MutableMapping, MutableSequence
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from ruamel.yaml import YAML


class GroupingMode(str, Enum):
    TASK = "task"
    STEP = "step"
    AGGREGATE = "aggregate"


class StyleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    legend: bool = True
    color_palette: str = Field(default="tab20", alias="color-palette")
    color_map: MutableMapping[str, str] = Field(default_factory=dict, alias="color-map")
    xlim: int | None = None
    grouping_mode: GroupingMode = Field(default="step", alias="grouping-mode")


def load_style_config(file_path: str) -> StyleConfig:
    yaml_loader = YAML(typ="safe")
    try:
        with open(file_path) as f:
            raw_data = yaml_loader.load(f) or {}
        return StyleConfig(**raw_data)
    except ValidationError as e:
        print(f"Error: Style configuration in '{file_path}' is invalid:")
        for error in e.errors():
            loc = " -> ".join(str(x) for x in error["loc"])
            print(f"  [{loc}]: {error['msg']}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading style file: {e}")
        sys.exit(1)


class OutputConfig:
    def __init__(
        self, outdir: str, filename: str, extension: MutableSequence[str]
    ) -> None:
        self.outdir: str = outdir
        self.filename: str = filename
        self.extension: MutableSequence[str] = extension

    def get_filepath(self, extension: str, prefix: str = "", postfix: str = "") -> str:
        filename = self.filename
        if not filename.endswith(f".{extension}"):
            filename = f"{prefix}{self.filename}{postfix}.{extension}"
        return os.path.join(self.outdir, filename)

    def get_statspath(self) -> str:
        return os.path.join(self.outdir, f"{self.filename}.stats.json")
