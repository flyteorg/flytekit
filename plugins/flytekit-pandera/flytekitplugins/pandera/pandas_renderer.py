from typing import Type, TYPE_CHECKING

from flytekit import lazy_module


if TYPE_CHECKING:
    import pandas
    import pandera
else:
    pandas = lazy_module("pandas")
    pandera = lazy_module("pandera")


class PandasReportRenderer:
    def __init__(self, title: str = "Pandera Error Report"):
        self._title = title

    def to_html(self, error: pandera.errors.SchemaErrors) -> str:
        error.failure_cases.groupby(["schema_context"])
        html = (
            error.failure_cases.set_index(["schema_context", "column", "check"])
            .drop(["check_number"], axis="columns")
            [["index", "failure_case"]]
            .to_html()
        )
        return html
