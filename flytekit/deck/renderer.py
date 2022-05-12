from typing import Any, Optional

import pandas
from typing_extensions import Protocol, runtime_checkable


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
