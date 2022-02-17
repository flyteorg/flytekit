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
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def render(self) -> str:
        if isinstance(self.dataframe, pd.DataFrame):
            return self.dataframe.to_html()


class FrameProfilingRenderer(Renderer):
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def render(self) -> str:
        if isinstance(self.dataframe, pd.DataFrame):
            profile = ProfileReport(self.dataframe, title="Pandas Profiling Report")
            return profile.to_html()


class MarkdownRenderer(Renderer):
    def __init__(self, text: str):
        self.text = text

    def render(self) -> str:
        return markdown.markdown(self.text)


class ScatterRenderer(Renderer):
    def __init__(self, x: Union[List, range], y: Union[List, range]):
        self.x = x
        self.y = y

    def render(self) -> str:
        fig = px.scatter(x=self.x, y=self.y)
        return fig.to_html()
