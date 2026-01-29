from typing import MutableSequence

from viewer.core.entity import Workflow
from viewer.core.utils import get_path
from viewer.translator.cwltool.log import translate_log


def cwltool_create_workflow(input_type: str, paths: MutableSequence[str]) -> Workflow:
    if input_type == "log":
        steps, workflow_start_date, workflow_end_date = translate_log(
            [get_path(path) for path in paths]
        )
        workflow = Workflow(workflow_start_date, workflow_end_date)
        workflow.steps.extend(steps)
        return workflow
    else:
        raise Exception("cwltool does not have an execution report")
