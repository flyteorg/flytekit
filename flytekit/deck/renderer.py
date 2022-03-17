import typing

import pandas as pd


class Renderable(typing.Protocol):
    def to_html(self) -> str:
        """Convert a object(markdown, pandas.dataframe) to HTML and return HTML as a unicode string.
        Returns: An HTML document as a string.
        """
        raise NotImplementedError


class TopFrameRenderer:
    """
    Render a DataFrame as an HTML table.
    """

    def __init__(self, max_rows: typing.Optional[int] = None):
        self._max_rows = max_rows

    def to_html(self, dataframe: pd.DataFrame) -> str:
        assert isinstance(dataframe, pd.DataFrame)
        return dataframe.to_html(max_rows=self._max_rows)
