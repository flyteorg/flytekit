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
