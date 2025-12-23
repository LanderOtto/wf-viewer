# Workflow Viewer (wf-viewer)

`wf-viewer` is a command-line tool designed to analyze and visualize workflow execution traces. It supports multiple workflow management systems and generates interactive Gantt charts and performance statistics.

## Installation

You can install `wf-viewer` directly from the source directory:

```bash
git clone https://github.com/alpha-unito/wf-viewer.git
cd wf-viewer
pip install .
```

## Quick Start

To generate a standard HTML Gantt chart from a StreamFlow JSON report:

```bash
wf-viewer -i streamflow.json -t report -w streamflow --stats
```

## Command Line Interface

The `wf-viewer` utility accepts the following arguments:

### Required Arguments

* `-i, --inputs <path>`: Path to input trace files. This flag can be passed multiple times to aggregate multiple execution logs.
* `-t, --input-type {report, log}`: The format of the input file.
* `-w, --workflow-manager {streamflow, cwltool, cwltoil}`: The manager that generated the logs.

### Output location

* `-n, --filename <str>`: Base name for the output file (default: `gantt`).
* `-o, --outdir <path>`: Target directory for output files (defaults to current working directory).

### Visualization Options

* `-f, --format {html, pdf, eps}`: Output file format. HTML provides interactive features (default: `html`).
* `-p, --color-palet <map>`: A [Matplotlib colormap](https://matplotlib.org/stable/gallery/color/colormap_reference.html) for task differentiation (default: `tab20`).
* `-g, --group-by-step {individual, aggregate}`: Defines how tasks are grouped in the visualization.
* `-l, --legend`: Toggle to display the color legend on the chart.

### Statistics & Logging

* `--stats`: Enables the generation of a `stats` file in the output directory.
* `--json-stats`: If `--stats` is present, saves the data in JSON format instead of the default custom text format.
* `--quiet`: Silences all standard output logging.

## Workflow Support Matrix

| Manager | Input Type | Description | Support status |
| --- | --- | --- |----------------|
| **StreamFlow** | `log` / `report` | Full support for task timelines and dependency mapping. | stable         |
| **CWLTool** | `log` | Standard CWL reference implementation logs. | WIP            |
| **CWLToil** | `log` | Support for Toil-specific execution traces. | WIP            |
