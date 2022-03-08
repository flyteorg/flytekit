from typing import List, Union

import markdown
import pandas
import plotly.express as px
from pandas_profiling import ProfileReport

from flytekit.deck.renderer import Renderer


class FrameProfilingRenderer(Renderer):
    def __init__(self, dataframe: pandas.DataFrame):
        self._dataframe = dataframe

    def render(self) -> str:
        if isinstance(self._dataframe, pandas.DataFrame):
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
