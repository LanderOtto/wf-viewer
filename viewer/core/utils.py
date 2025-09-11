from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path


def get_path(path: str) -> str:
    return str(Path(os.path.expanduser(os.path.expandvars(path))).absolute())


def str_to_datetime(date: str):
    result = None
    formats = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
    ]
    if date:
        if "." in date:
            main, frac = date.split(".", 1)
            # Remove timezone if exists in the fractional part
            frac_part = "".join(c for c in frac if c.isdigit())
            frac_truncated = frac_part[:6].ljust(6, "0")  # pad if shorter
            date = f"{main}.{frac_truncated}"
        for format in formats:
            try:
                return datetime.strptime(date, format)
            except ValueError:
                pass
        raise Exception(f"Not a valid date format for: {date}")
    return result
