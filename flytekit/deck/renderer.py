from typing import TYPE_CHECKING, Any

from markdown_it import MarkdownIt
from typing_extensions import Protocol, runtime_checkable

from flytekit import lazy_module

if TYPE_CHECKING:
    # Always import these modules in type-checking mode or when running pytest
    import pandas
    import pyarrow
else:
    pandas = lazy_module("pandas")
    pyarrow = lazy_module("pyarrow")


@runtime_checkable
class Renderable(Protocol):
    def to_html(self, python_value: Any) -> str:
        """Convert an object(markdown, pandas.dataframe) to HTML and return HTML as a unicode string.
        Returns: An HTML document as a string.
        """
        raise NotImplementedError


DEFAULT_MAX_ROWS = 10
DEFAULT_MAX_COLS = 100


class TopFrameRenderer:
    """
    Render a DataFrame as an HTML table.
    """

    def __init__(self, max_rows: int = DEFAULT_MAX_ROWS, max_cols: int = DEFAULT_MAX_COLS):
        self._max_rows = max_rows
        self._max_cols = max_cols

    def to_html(self, df: "pandas.DataFrame") -> str:
        assert isinstance(df, pandas.DataFrame)
        return df.to_html(max_rows=self._max_rows, max_cols=self._max_cols)


class ArrowRenderer:
    """
    Render an Arrow dataframe as an HTML table.
    """

    def to_html(self, df: "pyarrow.Table") -> str:
        assert isinstance(df, pyarrow.Table)
        return df.to_string()


class MarkdownRenderer:
    """Convert a markdown string to HTML and return HTML as a unicode string."""

    def to_html(self, text: str) -> str:
        return MarkdownIt().render(text)


class SourceCodeRenderer:
    """
    Convert Python source code to HTML, and return HTML as a unicode string.
    """

    def __init__(self, title: str = "Source Code"):
        self._title = title

    def to_html(self, source_code: str) -> str:
        """
        Convert the provided Python source code into HTML format using Pygments library.

        This method applies a colorful style and replaces the color "#fff0f0" with "#ffffff" in CSS.

        Args:
            source_code (str): The Python source code to be converted.

        Returns:
            str: The resulting HTML as a string, including CSS and highlighted source code.
        """
        from pygments import highlight
        from pygments.formatters.html import HtmlFormatter
        from pygments.lexers.python import PythonLexer

        formatter = HtmlFormatter(style="colorful")
        css = formatter.get_style_defs(".highlight").replace("#fff0f0", "#ffffff")
        html = highlight(source_code, PythonLexer(), formatter)
        return f"<style>{css}</style>{html}"


class PythonDependencyRenderer:
    """
    PythonDependencyDeck is a deck that contains information about packages installed via pip.
    """

    def __init__(self, title: str = "Dependencies"):
        self._title = title

    def to_html(self) -> str:
        import json
        import subprocess
        import sys

        from flytekit.loggers import logger

        try:
            installed_packages = json.loads(
                subprocess.check_output([sys.executable, "-m", "pip", "list", "--format", "json"])
            )
            requirements_txt = (
                subprocess.check_output([sys.executable, "-m", "pip", "freeze"])
                .decode("utf-8")
                .replace("\\n", "\n")
                .rstrip()
            )
        except Exception as e:
            logger.error(f"Error occurred while fetching installed packages: {e}")
            return "Error occurred while fetching installed packages."

        table = (
            "<table>\n<tr>\n<th style='text-align:left;'>Name</th>\n<th style='text-align:left;'>Version</th>\n</tr>\n"
        )

        for entry in installed_packages:
            table += f"<tr>\n<td>{entry['name']}</td>\n<td>{entry['version']}</td>\n</tr>\n"

        table += "</table>"

        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Flyte Dependencies</title>
        <script>
        async function copyTable() {{
          var requirements_txt = document.getElementById('requirements_txt');

          try {{
            await navigator.clipboard.writeText(requirements_txt.innerText);
          }} catch (err) {{
            console.log('Error accessing the clipboard: ' + err);
          }}
        }}
        </script>
        </head>
        <body>

        <button onclick="copyTable()">
          <span>Copy table as requirements.txt</span>
        </button>
        <h3>Python Dependencies</h3>

        {table}

        <div id="requirements_txt" style="display:none">{requirements_txt}</div>

        </body>
        </html>
        """
        return html
