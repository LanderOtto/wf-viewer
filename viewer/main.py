#!/usr/bin/python3

import sys

from viewer.cli.arguments import get_parser
from viewer.cli.priority import create_output_config, create_style_config
from viewer.render.report import create_report
from viewer.render.stats import create_stats
from viewer.translator.cwltool.manager import cwltool_create_workflow
from viewer.translator.streamflow.manager import sf_create_workflow
from viewer.translator.toil.manager import toil_create_workflow


def _main(args) -> int:
    style_config = create_style_config(args)
    out_config = create_output_config(args)

    workflow = None
    match args.workflow_manager:
        case "streamflow":
            workflow = sf_create_workflow(args.input_type, args.inputs)
        case "cwltool":
            workflow = cwltool_create_workflow(args.input_type, args.inputs)
        case "cwltoil":
            workflow = toil_create_workflow(args.input_type, args.inputs)
        case _:
            raise NotImplementedError(args.workflow_manager)

    create_stats(workflow, out_config, args.show_stats, args.save_stats)
    create_report(workflow, out_config, style_config)
    return 0


if __name__ == "__main__":
    parser = get_parser()
    try:
        sys.exit(_main(parser.parse_args()))
    except KeyboardInterrupt:
        sys.exit(130)
