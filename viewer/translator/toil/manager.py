from collections.abc import MutableSequence

from viewer.core.entity import Workflow
from viewer.core.utils import get_path
from viewer.translator.toil.log import translate_log


def toil_create_workflow(input_type: str, paths: MutableSequence[str]) -> Workflow:
    if len(paths) != 1:
        raise ValueError(f"Toil module does not support multiple input paths: {paths}")
    if input_type == "report":
        steps, workflow_start_date, workflow_end_date = translate_log(
            get_path(paths[0])
        )
        workflow = Workflow(workflow_start_date, workflow_end_date)
        workflow.steps.extend(steps)
        return workflow
    else:
        raise NotImplementedError
