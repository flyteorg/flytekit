import typing
from abc import ABC

import pandas as pd


class Renderer(ABC):
    def render(self) -> str:
        """Convert a object(markdown, pandas.dataframe) to HTML and return HTML as a unicode string.
        Returns: An HTML document as a string.
        """
        raise NotImplementedError


class FrameRenderer(Renderer):
    def __init__(self, dataframe: pd.DataFrame, max_rows: typing.Optional[int] = None):
        self._dataframe = dataframe
        self._max_rows = max_rows

    def render(self) -> str:
        if isinstance(self._dataframe, pd.DataFrame):
            return self._dataframe.to_html(max_rows=self._max_rows)
