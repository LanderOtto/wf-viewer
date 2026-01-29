from collections.abc import MutableSequence

from viewer.core.entity import Workflow
from viewer.core.utils import get_path
from viewer.translator.streamflow.log import translate_log
from viewer.translator.streamflow.report import translate_report


def sf_create_workflow(input_type: str, paths: MutableSequence[str]) -> Workflow:
    if len(paths) != 1:
        raise ValueError(
            f"StreamFlow module does not support multiple input paths: {paths}"
        )
    input_path = get_path(paths[0])
    if input_type == "report":
        return translate_report(input_path)
    elif input_type == "log":
        return translate_log(input_path)
    else:
        raise ValueError(f"Unknown input type: {input_type}")
