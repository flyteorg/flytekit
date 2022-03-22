from typing import Any, Optional

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import runtime_checkable, Protocol

import pandas


@runtime_checkable
class Renderable(Protocol):
    def to_html(self, python_value: Any) -> str:
        """Convert a object(markdown, pandas.dataframe) to HTML and return HTML as a unicode string.
        Returns: An HTML document as a string.
        """
        raise NotImplementedError


class TopFrameRenderer:
    """
    Render a DataFrame as an HTML table.
    """

    def __init__(self, max_rows: Optional[int] = None):
        self._max_rows = max_rows

    def to_html(self, df: pandas.DataFrame) -> str:
        assert isinstance(df, pandas.DataFrame)
        return df.to_html(max_rows=self._max_rows)
