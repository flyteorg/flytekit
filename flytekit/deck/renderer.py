import typing
from abc import ABC
from typing import List, Union

import markdown
import pandas as pd
import plotly.express as px
from pandas_profiling import ProfileReport


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


# Will move it to flytekitplugins-deck
class FrameProfilingRenderer(Renderer):
    def __init__(self, dataframe: pd.DataFrame):
        self._dataframe = dataframe

    def render(self) -> str:
        if isinstance(self._dataframe, pd.DataFrame):
            profile = ProfileReport(self._dataframe, title="Pandas Profiling Report")
            return profile.to_html()


class MarkdownRenderer(Renderer):
    def __init__(self, text: str):
        self._text = text

    def render(self) -> str:
        return markdown.markdown(self._text)


class ScatterRenderer(Renderer):
    # https://plotly.com/python/line-and-scatter/
    def __init__(self, x: Union[List, range], y: Union[List, range]):
        self._x = x
        self._y = y

    def render(self) -> str:
        fig = px.scatter(x=self._x, y=self._y)
        return fig.to_html()
