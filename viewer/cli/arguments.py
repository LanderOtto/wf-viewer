import argparse
import os

from viewer.cli.schema import GroupingMode


def get_parser():
    parser = argparse.ArgumentParser(description="Gantt Chart Generator")

    parser.add_argument(
        "style_config", nargs="?", help="Path to optional YAML configuration file"
    )

    # --- Group: Input Configuration ---
    input_group = parser.add_argument_group("Inputs")
    input_group.add_argument(
        "-i", "--inputs", action="append", required=True, help="Path to input files"
    )
    input_group.add_argument(
        "-t", "--input-type", choices=["report", "log"], required=True
    )
    input_group.add_argument(
        "-w",
        "--wms",
        dest="workflow_manager",
        choices=["streamflow", "cwltool", "cwltoil"],
        required=True,
    )
    input_group.add_argument(
        "-c", "--clusters-info", type=str, default=None, help="Path to cluster info"
    )

    # --- Group: Styling ---
    style_group = parser.add_argument_group("Style")
    style_group.add_argument(
        "-m",
        "--color-map",
        action="append",
        help="Format: StepName:Color (can be used multiple times)",
    )
    style_group.add_argument("-p", "--color-palette", type=str)
    style_group.add_argument(
        "-g",
        "--group-by",
        dest="grouping_mode",
        choices=[GroupingMode.TASK, GroupingMode.STEP, GroupingMode.AGGREGATE],
        default=None,
    )
    style_group.add_argument(
        "-l",
        "--legend",
        type=lambda x: (str(x).lower() == "true"),
        default=None,
        help="Enable/disable legend (true/false)",
    )
    style_group.add_argument("-x", "--xlim", type=float, default=None)

    # --- Group: Output Settings ---
    output_group = parser.add_argument_group("Outputs")
    output_group.add_argument("-n", "--filename", default="gantt")
    output_group.add_argument(
        "-f",
        "--format",
        choices=["html", "eps", "pdf", "png"],
        default=["html"],
        nargs="*",
        type=str,
        help="Report format: (default: html)",
    )
    output_group.add_argument("-o", "--outdir", default=os.getcwd())

    # --- Group: Statistics ---
    stats_group = parser.add_argument_group("Statistics & Logging")
    stats_group.add_argument(
        "--show-stats", action="store_true", help="Show statistics on stdout"
    )
    stats_group.add_argument(
        "--save-stats", action="store_true", help="Save statistics in a JSON file"
    )

    return parser
