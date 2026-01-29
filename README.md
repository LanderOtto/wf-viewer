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
wf-viewer -i streamflow.json -t report -w streamflow --show-stats
```

## Command Line Interface

The `wf-viewer` utility uses the following structure:
`wf-viewer [style_config] [arguments]`

### Positional Arguments

* `style_config`: (Optional) Path to a YAML configuration file to define visual styles.

### Inputs

* `-i, --inputs <path>`: Path to input trace files. This flag can be passed multiple times to aggregate multiple execution logs. **(Required)**
* `-t, --input-type {report, log}`: The format of the input file. **(Required)**
* `-w, --wms {streamflow, cwltool, cwltoil}`: The Workflow Management System that generated the logs. **(Required)**

### Style

* `-m, --color-map <StepName:Color>`: Explicitly map a step name to a specific color. Can be used multiple times.
* `-p, --color-palette <str>`: A [Matplotlib colormap](https://matplotlib.org/stable/gallery/color/colormap_reference.html) name for task differentiation.
* `-g, --group-by {task, step, aggregate}`: Defines the granularity of task grouping in the visualization.
* `-l, --legend {true, false}`: Explicitly enable or disable the legend.
* `-x, --xlim <float>`: Manually set the limit for the X-axis (time).

### Outputs

* `-n, --filename <str>`: Base name for the output file (default: `gantt`).
* `-o, --outdir <path>`: Target directory for output files (defaults to current working directory).
* `-f, --format {html, eps, pdf, png}`: Output file format. Multiple formats can be specified (default: `html`).

### Statistics & Logging

* `--show-stats`: Prints performance statistics directly to the standard output.
* `--save-stats`: Exports statistics data into a JSON file in the output directory.

---

## Workflow Support Matrix

| Manager | Input Type | Description | Support status |
| --- | --- | --- | --- |
| **StreamFlow** | `log` / `report` | Full support for task timelines and dependency mapping. | stable |
| **CWLTool** | `log` | Standard CWL reference implementation logs. | WIP |
| **CWLToil** | `log` | Support for Toil-specific execution traces. | WIP |
