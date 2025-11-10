#!/usr/bin/python3

import argparse
import json
import os
from pathlib import PurePath

from viewer.core.utils import get_path
from viewer.render.render import plot_gantt, show_analysis
from viewer.translator.cwltool import scraping_log
from viewer.translator.streamflow.log import get_metadata_from_log
from viewer.translator.streamflow.report import check_and_analysis, get_steps
from viewer.translator.toil import analysis


def main(args):
    if args.workflow_manager != "cwltool" and len(args.inputs) > 1:
        raise NotImplementedError(
            f"Only cwltool supports list of input files. Define a single input file for {args.workflow_manager}"
        )
    if args.workflow_manager == "streamflow":
        if args.input_type == "report":
            # Get report from json file
            with open(get_path(args.inputs[0])) as fd:
                data = json.load(fd)
            # Get Workflow start and end times
            workflow_start_date, workflow_end_date = check_and_analysis(data)
            # Get step times
            steps = get_steps(data, workflow_start_date)
        elif args.input_type == "log":
            steps, workflow_start_date, workflow_end_date = get_metadata_from_log(
                args.inputs[0]
            )
        else:
            raise Exception(f"Unknown input type: {args.input_type}")
    elif args.workflow_manager == "cwltool":
        if args.input_type == "report":
            raise Exception("cwltool does not have an execution report")
        elif args.input_type == "log":
            steps, workflow_start_date, workflow_end_date = scraping_log(
                [get_path(path) for path in args.inputs]
            )
        else:
            raise Exception(f"Unknown input type: {args.input_type}")
    elif args.workflow_manager == "cwltoil":
        if args.input_type == "report":
            steps, workflow_start_date, workflow_end_date = analysis(
                get_path(args.inputs[0])
            )
        elif args.input_type == "log":
            raise NotImplementedError
        else:
            raise Exception(f"Unknown input type: {args.input_type}")
    else:
        raise Exception(f"Invalid workflow manager: {args.workflow_manager}")
    stats_path = None
    if args.stats:
        stats_path = str(
            PurePath(
                get_path(args.outdir), "stats." + ("json" if args.json_stats else "txt")
            )
        )
    show_analysis(
        steps,
        workflow_start_date,
        workflow_end_date,
        stats_path,
        args.quiet,
        args.json_stats,
    )
    plot_gantt(
        steps, workflow_start_date, get_path(args.outdir), args.filename, args.format
    )


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-n",
            "--filename",
            help="filename",
            type=str,
            default="gantt",
        )
        parser.add_argument(
            "-f",
            "--format",
            help="filename",
            type=str,
            default="html",
            choices=["html"],
        )
        parser.add_argument(
            "-i",
            "--inputs",
            action="append",
            required=True,
            type=str,
        )
        parser.add_argument(
            "-o",
            "--outdir",
            help="Output directory",
            type=str,
            default=os.getcwd(),
        )
        parser.add_argument(
            "-t",
            "--input-type",
            help="type of input file",
            type=str,
            choices=["report", "log"],
            required=True,
        )
        parser.add_argument(
            "-w",
            "--workflow-manager",
            help="workflow manager output to analysis",
            type=str,
            choices=["streamflow", "cwltool", "cwltoil"],
            required=True,
        )

        parser.add_argument(
            "--stats",
            help="Create a file named 'stats' on the output directory. "
            "Default is used a custom format, you can used --json-stats",
            action="store_true",
        )

        parser.add_argument(
            "--json-stats",
            help="If is used in combination with --stats. Set the JSON as format of the stats",
            action="store_true",
        )
        parser.add_argument("--quiet", help="No logging on stdout", action="store_true")

        args = parser.parse_args()
        main(args)
    except KeyboardInterrupt:
        print()
