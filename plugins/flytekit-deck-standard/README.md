# Flytekit Deck Plugin

This plugin provides additional renderers to improve task visibility within Flytekit.

## Installation

To install the plugin, run the following command:

```bash
pip install flytekitplugins-deck-standard
```

## Renderer Requirements

Each renderer may require additional modules.

The table below outlines the dependencies for each renderer:

| Renderer               | Required Module(s)          |
|------------------------|-----------------------------|
| SourceCodeRenderer      | `pygments`                  |
| FrameProfilingRenderer  | `pandas`, `ydata-profiling` |
| MarkdownRenderer        | `markdown`                  |
| BoxRenderer             | `pandas`, `plotly`          |
| ImageRenderer           | `pillow`    |
| TableRenderer           | `pandas`                    |
| GanttChartRenderer      | `pandas`, `plotly`  |

## Renderer Descriptions

### SourceCodeRenderer
Converts Python source code to HTML using the Pygments library.

### FrameProfilingRenderer
Generates a profiling report based on a pandas DataFrame using `ydata_profiling`.

### MarkdownRenderer
Converts markdown strings to HTML.

### BoxRenderer
Creates a box-and-whisker plot from a column in a pandas DataFrame.

### ImageRenderer
Displays images from a `FlyteFile` or `PIL.Image.Image` object in HTML.

### TableRenderer
Renders a pandas DataFrame as an HTML table with customizable headers and table width.

### GanttChartRenderer
Displays a Gantt chart using a pandas DataFrame with "Start", "Finish", and "Name" columns.
