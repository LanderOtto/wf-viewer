from viewer.core.entity import Workflow
from viewer.translator.streamflow.log import translate_log
from viewer.translator.streamflow.report import translate_report


def sf_create_workflow(input_type: str, input_path: str) -> Workflow:
    if input_type == "report":
        return translate_report(input_path)
    elif input_type == "log":
        return translate_log(input_path)
    else:
        raise ValueError(f"Unknown input type: {input_type}")
