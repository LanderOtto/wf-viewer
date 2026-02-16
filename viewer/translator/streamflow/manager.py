from collections.abc import MutableMapping, MutableSequence
from typing import Any

from viewer.core.entity import Workflow
from viewer.core.utils import get_path
from viewer.translator.streamflow.log import translate_log
from viewer.translator.streamflow.report import translate_report


def sf_create_workflow(
    input_type: str,
    paths: MutableSequence[str],
    location_metadata: MutableMapping[str, Any],
) -> Workflow:
    if len(paths) != 1:
        raise ValueError(
            f"StreamFlow module does not support multiple input paths: {paths}"
        )
    input_path = get_path(paths[0])
    if input_type == "report":
        return translate_report(input_path)
    elif input_type == "log":
        return translate_log(input_path, location_metadata)
    else:
        raise ValueError(f"Unknown input type: {input_type}")
