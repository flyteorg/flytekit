from typing import Optional
import markdown
import pandas
import plotly.express as px
from pandas_profiling import ProfileReport
import whylogs as why
from whylogs import DatasetProfileView
from whylogs.viz import NotebookProfileVisualizer
from whylogs.core.constraints import Constraints


class FrameProfilingRenderer:
    """
    Generate a ProfileReport based on a pandas DataFrame
    """

    def __init__(self, title: str = "Pandas Profiling Report"):
        self._title = title

    def to_html(self, df: pandas.DataFrame) -> str:
        assert isinstance(df, pandas.DataFrame)
        profile = ProfileReport(df, title=self._title)
        return profile.to_html()


class MarkdownRenderer:
    """Convert a markdown string to HTML and return HTML as a unicode string.

    This is a shortcut function for `Markdown` class to cover the most
    basic use case.  It initializes an instance of Markdown, loads the
    necessary extensions and runs the parser on the given text.
    """

    def to_html(self, text: str) -> str:
        return markdown.markdown(text)


class BoxRenderer:
    """
    In a box plot, rows of `data_frame` are grouped together into a
    box-and-whisker mark to visualize their distribution.

    Each box spans from quartile 1 (Q1) to quartile 3 (Q3). The second
    quartile (Q2) is marked by a line inside the box. By default, the
    whiskers correspond to the box' edges +/- 1.5 times the interquartile
    range (IQR: Q3-Q1), see "points" for other options.
    """

    # More detail, see https://plotly.com/python/box-plots/
    def __init__(self, column_name):
        self._column_name = column_name

    def to_html(self, df: pandas.DataFrame) -> str:
        fig = px.box(df, y=self._column_name)
        return fig.to_html()


class WhylogsSummaryDriftRenderer:
    """
    Creates a whylogs' Summary Drift report from two pandas DataFrames. One of them
    is the reference and the other one is the target data, meaning that this is what 
    the report will compare it against.
    """
    @staticmethod
    def to_html(
        reference_data: pandas.DataFrame, 
        target_data: pandas.DataFrame
        ) -> str:
        """
        This static method will profile the input data and then generate an HTML report
        with the Summary Drift calculations for all of the dataframe's columns
        
        :param reference_data: The DataFrame that will be the reference for the drift report
        :type: pandas.DataFrame

        :param target_data: The data to compare against and create the Summary Drift report
        :type target_data: pandas.DataFrame        
        """
        
        target_view = why.log(target_data).view()
        reference_view = why.log(reference_data).view()
        viz = NotebookProfileVisualizer()
        viz.set_profiles(target_profile_view=target_view, reference_profile_view=reference_view)
        return viz.summary_drift_report().data


class WhylogsConstraintsRenderer:
    @staticmethod
    def to_html(constraints: Constraints) -> str:
        viz = NotebookProfileVisualizer()
        report = viz.constraints_report(constraints=constraints)
        return report.data
