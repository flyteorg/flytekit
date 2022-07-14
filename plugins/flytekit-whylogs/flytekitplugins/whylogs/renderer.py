import pandas as pd
import whylogs as why
from whylogs.viz import NotebookProfileVisualizer
from whylogs.core.constraints import Constraints


class WhylogsSummaryDriftRenderer:
    """
    Creates a whylogs' Summary Drift report from two pandas DataFrames. One of them
    is the reference and the other one is the target data, meaning that this is what
    the report will compare it against.
    """
    @staticmethod
    def to_html(
        reference_data: pd.DataFrame,
        target_data: pd.DataFrame
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
