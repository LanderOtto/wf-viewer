#!/usr/bin/python3

import sys
from pathlib import PurePath

from viewer.cli.arguments import get_parser
from viewer.cli.priority import create_output_config, create_style_config
from viewer.core.entity import Workflow
from viewer.core.utils import get_path
from viewer.render.report import create_report
from viewer.render.stats import create_stats
from viewer.translator.cwltool import scraping_log
from viewer.translator.streamflow.manager import sf_create_workflow
from viewer.translator.toil import analysis


def _main(args) -> int:
    if args.workflow_manager != "cwltool" and len(args.inputs) > 1:
        raise NotImplementedError("Only cwltool supports multiple inputs.")
    style_config = create_style_config(args)
    out_config = create_output_config(args)

    workflow = None
    match args.workflow_manager:
        case "streamflow":
            workflow = sf_create_workflow(args.input_type, get_path(args.inputs[0]))
        case "cwltool":
            if args.input_type == "log":
                steps, workflow_start_date, workflow_end_date = scraping_log(
                    [get_path(path) for path in args.inputs]
                )
                workflow = Workflow(workflow_start_date, workflow_end_date)
                workflow.steps.extend(steps)
            else:
                raise Exception("cwltool does not have an execution report")

        case "cwltoil":
            if args.input_type == "report":
                steps, workflow_start_date, workflow_end_date = analysis(
                    get_path(args.inputs[0])
                )
                workflow = Workflow(workflow_start_date, workflow_end_date)
                workflow.steps.extend(steps)
            else:
                raise NotImplementedError

        case _:
            raise NotImplementedError

    stats_path = None
    if args.stats:
        ext = "json" if args.json_stats else "txt"
        stats_path = str(PurePath(get_path(args.outdir), f"stats.{ext}"))

    create_stats(
        workflow.steps,
        workflow.start_date,
        workflow.end_date,
        stats_path,
        args.quiet,
        args.json_stats,
    )

    create_report(workflow=workflow, out_config=out_config, style_config=style_config)
    return 0


if __name__ == "__main__":
    parser = get_parser()
    try:
        sys.exit(_main(parser.parse_args()))
    except KeyboardInterrupt:
        sys.exit(130)
