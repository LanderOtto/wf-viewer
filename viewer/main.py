#!/usr/bin/python3

import argparse
import json
import os
import sys
from pathlib import PurePath

from viewer.core.utils import get_path
from viewer.render.render import plot_gantt, show_analysis
from viewer.translator.cwltool import scraping_log
from viewer.translator.streamflow.log import get_metadata_from_log
from viewer.translator.streamflow.report import check_and_analysis, get_steps
from viewer.translator.toil import analysis


def _main(args) -> int:
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
    if len(steps) == 0:
        raise Exception("No steps found")
    plot_gantt(
        steps=steps,
        workflow_start_date=workflow_start_date,
        color_palet=args.color_palet,
        group_by_step=args.group_by_step,
        outdir=get_path(args.outdir),
        filename=args.filename,
        format=args.format,
        args=args,
    )
    return 0


def main(args) -> int:
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-n",
            "--filename",
            help="The base name for the output file (default: %(default)s)",
            type=str,
            default="gantt",
        )
        parser.add_argument(
            "-f",
            "--format",
            help="The file format for the output chart (default: %(default)s)",
            type=str,
            default="html",
            choices=["html", "pdf", "eps"],
        )
        parser.add_argument(
            "-i",
            "--inputs",
            action="append",
            required=True,
            type=str,
            help="Path to input files. Can be used multiple times to include multiple logs.",
        )
        parser.add_argument(
            "-p",
            "--color-palet",
            type=str,
            default="tab20",
            help="Matplotlib color palette to use (see: https://matplotlib.org/stable/gallery/color/colormap_reference.html)",
        )
        parser.add_argument(
            "-g",
            "--group-by-step",
            help="Method to group workflow steps in the visualization",
            type=str,
            default=None,
            choices=["individual", "aggregate"],
        )
        parser.add_argument(
            "-l",
            "--legend",
            help="Include a legend in the generated chart",
            action="store_true",
        )
        parser.add_argument(
            "-o",
            "--outdir",
            help="Directory where results will be saved (default: current directory)",
            type=str,
            default=os.getcwd(),
        )
        parser.add_argument(
            "-t",
            "--input-type",
            help="The format of the input file(s) being processed",
            type=str,
            choices=["report", "log"],
            required=True,
        )
        parser.add_argument(
            "-w",
            "--workflow-manager",
            help="The workflow engine that produced the input logs",
            type=str,
            choices=["streamflow", "cwltool", "cwltoil"],
            required=True,
        )

        parser.add_argument(
            "--stats",
            help="Generate a statistics file in the output directory. Defaults to a custom text format.",
            action="store_true",
        )

        parser.add_argument(
            "--json-stats",
            help="Format the statistics file as JSON (must be used in combination with --stats)",
            action="store_true",
        )
        parser.add_argument(
            "--quiet", help="Suppress all logging output to stdout", action="store_true"
        )

        args_parsed = parser.parse_args(args)
        return _main(args_parsed)
    except KeyboardInterrupt:
        print()
    return 1


def run() -> int:
    return main(sys.argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
