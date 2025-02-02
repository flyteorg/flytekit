import warnings
from typing import TYPE_CHECKING, List, Optional, Union

from flytekit import lazy_module
from flytekit.types.file import FlyteFile
from ._memory_viz import (
    trace_plot, 
    segment_plot, 
    memory, 
    segments, 
    compare, 
    profile_plot,
    _format_size,  # Could be useful for formatting memory sizes
    segsum,        # Could be useful for summary visualization
    trace          # Could be useful for detailed trace visualization
)
import pickle

if TYPE_CHECKING:
    import markdown
    import pandas as pd
    import PIL.Image
    import pygments
    import ydata_profiling
else:
    pd = lazy_module("pandas")
    markdown = lazy_module("markdown")
    PIL = lazy_module("PIL")
    ydata_profiling = lazy_module("ydata_profiling")
    pygments = lazy_module("pygments")


class SourceCodeRenderer:
    """
    Convert Python source code to HTML, and return HTML as a unicode string.
    """

    def __init__(self, title: str = "Source Code"):
        self._title = title
        msg = (
            "flytekitplugins.deck.SourceCodeRenderer is deprecated. Please use flytekit.deck.SourceCodeRenderer instead"
        )
        warnings.warn(msg, FutureWarning)

    def to_html(self, source_code: str) -> str:
        """
        Convert the provided Python source code into HTML format using Pygments library.

        This method applies a colorful style and replaces the color "#fff0f0" with "#ffffff" in CSS.

        Args:
            source_code (str): The Python source code to be converted.

        Returns:
            str: The resulting HTML as a string, including CSS and highlighted source code.
        """
        formatter = pygments.formatters.html.HtmlFormatter(style="colorful")
        css = formatter.get_style_defs(".highlight").replace("#fff0f0", "#ffffff")
        html = pygments.highlight(source_code, pygments.lexers.python.PythonLexer(), formatter)
        return f"<style>{css}</style>{html}"


class FrameProfilingRenderer:
    """
    Generate a ProfileReport based on a pandas DataFrame
    """

    def __init__(self, title: str = "Pandas Profiling Report"):
        self._title = title

    def to_html(self, df: "pd.DataFrame") -> str:
        assert isinstance(df, pd.DataFrame)
        profile = ydata_profiling.ProfileReport(df, title=self._title)
        return profile.to_html()


class MarkdownRenderer:
    """Convert a markdown string to HTML and return HTML as a unicode string.

    This is a shortcut function for `Markdown` class to cover the most
    basic use case.  It initializes an instance of Markdown, loads the
    necessary extensions and runs the parser on the given text.
    """

    def __init__(self):
        msg = "flytekitplugins.deck.MarkdownRenderer is deprecated. Please use flytekit.deck.MarkdownRenderer instead"
        warnings.warn(msg, FutureWarning)

    def to_html(self, text: str) -> str:
        return markdown.markdown(text)


class BoxRenderer:
    """
    In a box plot, rows of `data_frame` are grouped together into a
    box-and-whisker mark to visualize their distribution.

    Each box spans from quartile 1 (Q1) to quartile 3 (Q3). The second
    quartile (Q2) is marked by a line inside the box. By default, the
    whiskers correspond to the box edges +/- 1.5 times the interquartile
    range (IQR: Q3-Q1), see "points" for other options.
    """

    # More detail, see https://plotly.com/python/box-plots/
    def __init__(self, column_name):
        self._column_name = column_name

    def to_html(self, df: "pd.DataFrame") -> str:
        import plotly.express as px

        fig = px.box(df, y=self._column_name)
        return fig.to_html()


class ImageRenderer:
    """Converts a FlyteFile or PIL.Image.Image object to an HTML string with the image data
    represented as a base64-encoded string.
    """

    def to_html(self, image_src: Union[FlyteFile, "PIL.Image.Image"]) -> str:
        img = self._get_image_object(image_src)
        return self._image_to_html_string(img)

    @staticmethod
    def _get_image_object(image_src: Union[FlyteFile, "PIL.Image.Image"]) -> "PIL.Image.Image":
        if isinstance(image_src, FlyteFile):
            local_path = image_src.download()
            return PIL.Image.open(local_path)
        elif isinstance(image_src, PIL.Image.Image):
            return image_src
        else:
            raise ValueError("Unsupported image source type")

    @staticmethod
    def _image_to_html_string(img: "PIL.Image.Image") -> str:
        import base64
        from io import BytesIO

        buffered = BytesIO()
        img.save(buffered, format="PNG")
        img_base64 = base64.b64encode(buffered.getvalue()).decode()
        return f'<img src="data:image/png;base64,{img_base64}" alt="Rendered Image" />'


class TableRenderer:
    """
    Convert a pandas DataFrame into an HTML table.
    """

    def to_html(
        self, df: "pd.DataFrame", header_labels: Optional[List] = None, table_width: Optional[int] = None
    ) -> str:
        # Check if custom labels are provided and have the correct length
        if header_labels is not None and len(header_labels) == len(df.columns):
            df = df.copy()
            df.columns = header_labels

        style = f"""
            <style>
                .table-class {{
                    border: 1px solid #ccc;  /* Add a thin border around the table */
                    border-collapse: collapse;
                    font-family: Arial, sans-serif;
                    color: #333;
                    {f'width: {table_width}px;' if table_width is not None else ''}
                }}

                .table-class th, .table-class td {{
                    border: 1px solid #ccc;  /* Add a thin border around each cell */
                    padding: 8px;  /* Add some padding inside each cell */
                }}

                /* Set the background color for even rows */
                .table-class tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}

                /* Add a hover effect to the rows */
                .table-class tr:hover {{
                    background-color: #ddd;
                }}

                /* Center the column headers */
                .table-class th {{
                    text-align: center;
                }}
            </style>
        """
        return style + df.to_html(classes="table-class", index=False)


class GanttChartRenderer:
    """
    This renderer is primarily used by the timeline deck. The input DataFrame should
    have at least the following columns:
    - "Start": datetime.datetime (represents the start time)
    - "Finish": datetime.datetime (represents the end time)
    - "Name": string (the name of the task or event)
    """

    def to_html(self, df: "pd.DataFrame", chart_width: Optional[int] = None) -> str:
        import plotly.express as px

        fig = px.timeline(df, x_start="Start", x_end="Finish", y="Name", color="Name", width=chart_width)

        fig.update_xaxes(
            tickangle=90,
            rangeslider_visible=True,
            tickformatstops=[
                dict(dtickrange=[None, 1], value="%3f ms"),
                dict(dtickrange=[1, 60], value="%S:%3f s"),
                dict(dtickrange=[60, 3600], value="%M:%S m"),
                dict(dtickrange=[3600, None], value="%H:%M h"),
            ],
        )

        # Remove y-axis tick labels and title since the time line deck space is limited.
        fig.update_yaxes(showticklabels=False, title="")

        fig.update_layout(
            autosize=True,
            # Set the orientation of the legend to horizontal and move the legend anchor 2% beyond the top of the timeline graph's vertical axis
            legend=dict(orientation="h", y=1.02),
        )

        return fig.to_html()

class PyTorchProfilingRenderer:
    """Renders PyTorch profiling data in various visualization formats.
    
    This renderer is particularly useful for analyzing memory usage and potential
    memory-related failures in PyTorch executions. It can help diagnose OOM (Out of Memory)
    errors and memory leaks by providing various visualization types.
    
    Supports multiple visualization types:
    - trace_plot: Shows the execution timeline
    - segment_plot: Shows the execution segments
    - memory: Displays memory usage over time
    - segments: Shows detailed segment information
    - compare: Compares two profiling snapshots
    - profile_plot: Shows detailed profiling information
    - summary: Shows overall allocation statistics
    - trace_view: Shows detailed trace information
    
    The renderer can be particularly helpful in:
    1. Analyzing failed executions due to OOM errors
    2. Identifying memory leaks
    3. Understanding memory usage patterns
    4. Comparing memory states before and after operations
    """
    def __init__(self, profiling_data):
        if profiling_data is None:
            raise ValueError("Profiling data cannot be None")
            
        # Handle both single snapshot and comparison cases
        if isinstance(profiling_data, tuple):
            before, after = profiling_data
            if before is None or after is None:
                raise ValueError("Both before and after snapshots must be provided for comparison")
            
            # Check if this might be an OOM case by comparing memory usage
            try:
                self._check_memory_growth(before, after)
            except Exception:
                # Don't fail initialization if memory check fails
                pass
        
        self.profiling_data = profiling_data

    def _check_memory_growth(self, before, after):
        """Check for significant memory growth between snapshots"""
        before_mem = self._get_total_memory(before)
        after_mem = self._get_total_memory(after)
        if after_mem > before_mem * 1.5:  # 50% growth threshold
            warnings.warn(
                f"Significant memory growth detected: {_format_size(before_mem)} -> {_format_size(after_mem)}",
                RuntimeWarning
            )

    def _get_total_memory(self, snapshot):
        """Get total memory usage from a snapshot"""
        total = 0
        for seg in snapshot.get("segments", []):
            total += seg.get("total_size", 0)
        return total

    def get_failure_analysis(self) -> str:
        """
        Analyze profiling data for potential failure causes.
        Particularly useful for OOM and memory-related failures.
        
        Returns:
            str: HTML formatted analysis of potential issues
        """
        analysis = []
        
        # Get memory summary
        memory_summary = self.get_memory_summary()
        analysis.append("<h3>Memory Usage Summary</h3>")
        analysis.append(f"<pre>{memory_summary}</pre>")
        
        # Get trace summary for context
        trace_summary = self.get_trace_summary()
        analysis.append("<h3>Execution Trace Summary</h3>")
        analysis.append(f"<pre>{trace_summary}</pre>")
        
        # Add memory visualization
        analysis.append("<h3>Memory Usage Visualization</h3>")
        analysis.append(self.to_html("memory"))
        
        return "\n".join(analysis)

    def get_memory_metrics(self) -> dict:
        """
        Get key memory metrics that might be useful for failure analysis
        
        Returns:
            dict: Dictionary containing memory metrics
        """
        metrics = {
            "peak_memory": 0,
            "total_allocations": 0,
            "largest_allocation": 0,
            "memory_at_failure": 0,
        }
        
        try:
            # Extract metrics from profiling data
            if isinstance(self.profiling_data, tuple):
                # For comparison case, use the 'after' snapshot
                data = self.profiling_data[1]
            else:
                data = self.profiling_data
                
            for seg in data.get("segments", []):
                metrics["peak_memory"] = max(metrics["peak_memory"], seg.get("total_size", 0))
                for block in seg.get("blocks", []):
                    if block.get("state") == "active_allocated":
                        metrics["total_allocations"] += 1
                        metrics["largest_allocation"] = max(
                            metrics["largest_allocation"],
                            block.get("size", 0)
                        )
            
            # Get the last known memory state
            metrics["memory_at_failure"] = self._get_total_memory(data)
            
        except Exception as e:
            warnings.warn(f"Failed to extract memory metrics: {str(e)}")
            
        return metrics

    def format_memory_size(self, size: int) -> str:
        """Format memory size using the _memory_viz helper"""
        return _format_size(size)

    def to_html(self, plot_type: str = "trace_plot") -> str:
        """Convert profiling data to HTML visualization."""
        
        # Define memory_viz_js at the start so it's available for all branches
        memory_viz_js = """
        <script src="MemoryViz.js" type="text/javascript"></script>
        <script type="text/javascript">
            function init(evt) {
                if (window.svgDocument == null) {
                    svgDocument = evt.target.ownerDocument;
                }
            }
        </script>
        """
        
        if plot_type == "profile_plot":
            import torch
            try:
                # Create a profile object without initializing it
                profile = torch.profiler.profile()
                # Set basic attributes needed for visualization
                profile.steps = []
                profile.events = []
                profile.key_averages = []
                
                # Copy the data from our profiling_data
                if isinstance(self.profiling_data, dict):
                    for key, value in self.profiling_data.items():
                        setattr(profile, key, value)
                content = profile_plot(profile)
            except Exception as e:
                content = f"<div>Failed to generate profile plot: {str(e)}</div>"
        
        elif plot_type == "compare":
            if not isinstance(self.profiling_data, tuple):
                raise ValueError("Compare plot type requires before/after snapshots")
            before, after = self.profiling_data
            
            # Ensure both snapshots have the correct structure
            def ensure_structure(data):
                if isinstance(data, dict) and "segments" in data:
                    return data
                return {
                    "segments": data.get("segments", []) if hasattr(data, "get") else [],
                    "traces": data.get("traces", []) if hasattr(data, "get") else [],
                    "allocator_settings": data.get("allocator_settings", {}) if hasattr(data, "get") else {}
                }
            
            before = ensure_structure(before)
            after = ensure_structure(after)
            content = compare(before["segments"], after["segments"])
        
        elif plot_type == "trace_plot":
            content = trace_plot(self.profiling_data)
        elif plot_type == "segment_plot":
            content = segment_plot(self.profiling_data)
        elif plot_type == "memory":
            content = memory(self.profiling_data)
        elif plot_type == "segments":
            content = segments(self.profiling_data)
        else:
            raise ValueError(f"Unknown plot type: {plot_type}")

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>PyTorch Memory Profiling</title>
            {memory_viz_js if plot_type in ["memory", "segments", "compare"] else ""}
        </head>
        <body>
            {content}
        </body>
        </html>
        """
    
    def get_memory_summary(self) -> str:
        """Get a text summary of memory usage"""
        return segsum(self.profiling_data)

    def get_trace_summary(self) -> str:
        """Get a text summary of the trace"""
        return trace(self.profiling_data)

    @staticmethod
    def load_from_file(file_path: str) -> 'PyTorchProfilingRenderer':
        """Create a renderer instance from a pickle file"""
        try:
            with open(file_path, "rb") as f:
                profiling_data = pickle.load(f)
            return PyTorchProfilingRenderer(profiling_data)
        except Exception as e:
            raise ValueError(f"Failed to load profiling data: {str(e)}")

def render_pytorch_profiling(profiling_file: FlyteFile,  plot_type: str = "trace_plot") -> str:
    """Renders PyTorch profiling data from a pickle file into HTML visualization.
    
    Args:
        profiling_file (FlyteFile): Pickle file containing PyTorch profiling data
        plot_type (str): Type of visualization to generate
        
    Returns:
        str: HTML string containing the visualization
        
    Raises:
        FileNotFoundError: If profiling file doesn't exist
        ValueError: If plot type is invalid or data loading fails
    """
    # Load the profiling data from the .pkl file
    try:
        with open(profiling_file, "rb") as f:
            profiling_data = pickle.load(f)
    except Exception as e:
        raise ValueError(f"Failed to load profiling data: {str(e)}")
    # Create an instance of the renderer and generate the HTML
    renderer = PyTorchProfilingRenderer(profiling_data)
    return renderer.to_html(plot_type)

def test_compare_plot_type():
    """Test the compare plot type which requires two snapshots"""
    with open(PROFILE_PATH, "rb") as f:
        profiling_data = pickle.load(f)
    
    # Create proper before/after snapshots
    before = {"segments": [], "traces": []}  # Empty snapshot
    after = profiling_data  # Your actual data
    
    renderer = PyTorchProfilingRenderer((before, after))
    html_output = renderer.to_html("compare")
    
    assert isinstance(html_output, str)
    assert "<html>" in html_output