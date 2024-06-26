#!/usr/bin/python3

import argparse
import json
import os

from viewer.core.utils import get_path
from viewer.render.render import plot_gantt, print_to_stdout
from viewer.translator.cwltool import scraping_log
from viewer.translator.streamflow import check_and_analysis, get_steps


def main(args):
    if args.workflow_manager == "streamflow":
        if args.input_type == "report":
            # Get report from json file
            with open(get_path(args.input)) as fd:
                data = json.load(fd)
            # Get Workflow start and end times
            workflow_start_date, workflow_end_date = check_and_analysis(data)
            # Get step times
            steps = get_steps(data, workflow_start_date)
        elif args.input_type == "log":
            raise NotImplementedError("WIP. StreamFlow-log")
        else:
            raise Exception(f"Unknown input type: {args.input_type}")
    elif args.workflow_manager == "cwltool":
        if args.input_type == "report":
            raise Exception("cwltool does not have an execution report")
        elif args.input_type == "log":
            steps, workflow_start_date, workflow_end_date = scraping_log(
                get_path(args.input)
            )
        else:
            raise Exception(f"Unknown input type: {args.input_type}")
    else:
        raise Exception(f"Invalid workflow manager: {args.workflow_manager}")
    print_to_stdout(steps, workflow_start_date, workflow_end_date)
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
            "--input",
            help="Insert path file",
            type=str,
            required=True,
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
            choices=["streamflow", "cwltool"],
            required=True,
        )
        args = parser.parse_args()
        main(args)
    except KeyboardInterrupt:
        print()
