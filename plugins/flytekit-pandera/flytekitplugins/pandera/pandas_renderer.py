from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from flytekit import lazy_module

if TYPE_CHECKING:
    import great_tables as gt
    import pandas

    import pandera
else:
    gt = lazy_module("great_tables")
    pandas = lazy_module("pandas")
    pandera = lazy_module("pandera")


@dataclass
class PandasReport:
    summary: pandas.DataFrame
    data_preview: pandas.DataFrame
    schema_error_df: Optional[pandas.DataFrame] = None
    data_error_df: Optional[pandas.DataFrame] = None


SCHEMA_ERROR_KEY = "SCHEMA"
DATA_ERROR_KEY = "DATA"
SCHEMA_ERROR_COLUMNS = ["schema", "column", "error_code", "check", "failure_case", "error"]
DATA_ERROR_COLUMNS = ["schema", "column", "error_code", "check", "index", "failure_case", "error"]
DATA_ERROR_DISPLAY_ORDER = ["column", "error_code", "percent_valid", "check", "failure_cases", "error"]

DATA_PREVIEW_HEAD = 5
FAILURE_CASE_LIMIT = 10
ERROR_COLUMN_MAX_WIDTH = 200


class PandasReportRenderer:
    def __init__(self, title: str = "Pandera Error Report"):
        self._title = title

    def _create_success_report(self, data: "pandas.DataFrame", schema: pandera.DataFrameSchema):
        summary = pandas.DataFrame(
            [
                {"Metadata": "Schema Name", "Value": schema.name},
                {"Metadata": "Shape", "Value": f"{data.shape[0]} rows x {data.shape[1]} columns"},
                {"Metadata": "Total schema errors", "Value": 0},
                {"Metadata": "Total data errors", "Value": 0},
                {"Metadata": "Schema Object", "Value": f"```\n{schema.__repr__()}\n```"},
            ]
        )

        return PandasReport(
            summary=summary,
            data_preview=data.head(DATA_PREVIEW_HEAD),
            schema_error_df=None,
            data_error_df=None,
        )

    @staticmethod
    def _reshape_long_failure_cases(long_failure_cases: "pandas.DataFrame"):
        return (
            long_failure_cases.pivot(
                index=["schema_context", "check", "index"], columns="column", values="failure_case"
            )
            .apply(lambda s: s.to_dict(), axis="columns")
            .rename("failure_case")
            .reset_index(["index", "check"])
            .reset_index(drop=True)[["check", "index", "failure_case"]]
        )

    def _prepare_data_error_df(self, data: "pandas.DataFrame", data_errors: dict, failure_cases: "pandas.DataFrame"):
        def num_failure_cases(series):
            return len(series)

        def _failure_cases(series):
            series = series.astype(str)
            out = ", ".join(str(x) for x in series.iloc[:FAILURE_CASE_LIMIT])
            if len(series) > FAILURE_CASE_LIMIT:
                out += f" ... (+{len(series) - FAILURE_CASE_LIMIT} more)"
            return out

        data_errors = pandas.concat(pandas.DataFrame(v).assign(error_code=k) for k, v in data_errors.items())
        # this is a bit of a hack to get the failure cases into the same format as the data errors
        long_failure_case_selector = (failure_cases["schema_context"] == "DataFrameSchema") & (
            failure_cases["column"].notna()
        )
        long_failure_cases = failure_cases[long_failure_case_selector]

        data_error_df = [data_errors.merge(failure_cases, how="inner", on=["column", "check"])]
        if long_failure_cases.shape[0] > 0:
            reshaped_failure_cases = self._reshape_long_failure_cases(long_failure_cases)
            long_data_errors = data_errors.assign(
                column=data_errors.column.where(~(data_errors.column == data_errors.schema), "NA")
            )
            data_error_df.append(
                long_data_errors.merge(reshaped_failure_cases, how="inner", on=["check"]).assign(column="NA")
            )

        data_error_df = pandas.concat(data_error_df)
        out_df = (
            data_error_df[DATA_ERROR_COLUMNS]
            .groupby(["column", "error_code", "check", "error"])
            .failure_case.agg([num_failure_cases, _failure_cases])
            .reset_index()
            .rename(columns={"_failure_cases": "failure_cases"})
            .assign(percent_valid=lambda df: 1 - (df["num_failure_cases"] / data.shape[0]))
        )
        return out_df

    def _create_error_report(
        self,
        data: "pandas.DataFrame",
        schema: pandera.DataFrameSchema,
        error: "pandera.errors.SchemaErrors",
    ):
        failure_cases = error.failure_cases
        error_dict = error.args[0]

        schema_errors = error_dict.get(SCHEMA_ERROR_KEY)
        data_errors = error_dict.get(DATA_ERROR_KEY)

        if schema_errors is None:
            schema_error_df = None
            total_schema_errors = 0
        else:
            schema_error_df = (
                pandas.concat(pandas.DataFrame(v).assign(error_code=k) for k, v in schema_errors.items())
                .merge(failure_cases, how="left", on=["column", "check"])[SCHEMA_ERROR_COLUMNS]
                .drop(["schema"], axis="columns")
            )
            total_schema_errors = schema_error_df.shape[0]

        if data_errors is None:
            data_error_df = None
            total_data_errors = 0
        else:
            data_error_df = self._prepare_data_error_df(data, data_errors, failure_cases)
            total_data_errors = data_error_df.shape[0]

        summary = pandas.DataFrame(
            [
                {"Metadata": "Schema Name", "Value": schema.name},
                {"Metadata": "Data Shape", "Value": f"{data.shape[0]} rows x {data.shape[1]} columns"},
                {"Metadata": "Total schema errors", "Value": total_schema_errors},
                {"Metadata": "Total data errors", "Value": total_data_errors},
                {"Metadata": "Schema Object", "Value": f"```\n{error.schema.__repr__()}\n```"},
            ]
        )

        return PandasReport(
            summary=summary,
            data_preview=data.head(DATA_PREVIEW_HEAD),
            schema_error_df=schema_error_df,
            data_error_df=data_error_df,
        )

    def _format_summary_df(self, df: "pandas.DataFrame") -> str:
        return (
            gt.GT(df)
            .tab_header(
                title=gt.md("**Summary**"),
                subtitle="A high-level overview of the schema errors found in the DataFrame.",
            )
            .cols_width(
                cases={
                    "Metadata": "20%",
                    "Value": "80%",
                }
            )
            .fmt_markdown(["Value"])
            .tab_stub(rowname_col="Metadata")
            .tab_stubhead(label="Metadata")
            .tab_style(style=gt.style.text(align="left"), locations=gt.loc.header())
            .tab_style(style=gt.style.fill(color="#f2fae2"), locations=gt.loc.header())
            .tab_style(
                style=gt.style.text(weight="bold"),
                locations=[gt.loc.column_labels(), gt.loc.stubhead(), gt.loc.stub()],
            )
            .as_raw_html()
        )

    def _format_data_preview_df(self, df: "pandas.DataFrame") -> str:
        return (
            gt.GT(df)
            .tab_header(
                title=gt.md("**Data Preview**"),
                subtitle=f"A preview of the first {min(DATA_PREVIEW_HEAD, df.shape[0])} rows of the data.",
            )
            .tab_style(style=gt.style.text(align="left"), locations=gt.loc.header())
            .tab_style(style=gt.style.fill(color="#f2fae2"), locations=gt.loc.header())
            .tab_style(style=gt.style.text(weight="bold"), locations=gt.loc.column_labels())
            .tab_style(style=gt.style.text(align="left"), locations=[gt.loc.body(), gt.loc.column_labels()])
            .as_raw_html()
        )

    @staticmethod
    def _format_error(x: str) -> str:
        if len(x) > ERROR_COLUMN_MAX_WIDTH:
            x = f"{x[:ERROR_COLUMN_MAX_WIDTH]}..."
        return f"```\n{x}\n```"

    def _format_schema_error_df(self, df: "pandas.DataFrame") -> str:
        df = df.assign(
            error=lambda df: df["error"].map(self._format_error),
            error_code=lambda df: df["error_code"].map(lambda x: f"`{x}`"),
            check=lambda df: df["check"].map(lambda x: f"`{x}`"),
        )
        return (
            gt.GT(df)
            .tab_header(
                title=gt.md("**Schema-level Errors**"),
                subtitle="Schema-level metadata errors, e.g. column names, dtypes.",
            )
            .fmt_markdown(["error_code", "check", "error"])
            .tab_style(style=gt.style.text(align="left"), locations=gt.loc.header())
            .tab_style(style=gt.style.fill(color="#f2fae2"), locations=gt.loc.header())
            .tab_style(style=gt.style.text(weight="bold"), locations=gt.loc.column_labels())
            .as_raw_html()
        )

    def _format_data_error_df(self, df: "pandas.DataFrame") -> str:
        df = df.assign(
            error=lambda df: df["error"].map(self._format_error),
            error_code=lambda df: df["error_code"].map(lambda x: f"`{x}`"),
            check=lambda df: df["check"].map(lambda x: f"`{x}`"),
        )[DATA_ERROR_DISPLAY_ORDER]

        return (
            gt.GT(df)
            .tab_header(
                title=gt.md("**Data-level Errors**"),
                subtitle="Data-level value errors, e.g. null values, out-of-range values.",
            )
            .fmt_markdown(["error_code", "check", "error"])
            .fmt_percent("percent_valid", decimals=2)
            .data_color(columns=["percent_valid"], palette="RdYlGn", domain=[0, 1], alpha=0.2)
            .tab_stub(groupname_col="column", rowname_col="error_code")
            .tab_stubhead(label="column")
            .tab_style(
                style=gt.style.text(align="left"), locations=[gt.loc.header(), gt.loc.column_labels(), gt.loc.body()]
            )
            .tab_style(style=gt.style.text(align="center"), locations=gt.loc.body(columns="percent_valid"))
            .tab_style(style=gt.style.fill(color="#f2fae2"), locations=gt.loc.header())
            .tab_style(
                style=gt.style.text(weight="bold"),
                locations=[gt.loc.column_labels(), gt.loc.stubhead(), gt.loc.row_groups()],
            )
            .tab_style(
                style=gt.style.fill(color="#f4f4f4"),
                locations=[gt.loc.row_groups(), gt.loc.stub()],
            )
            .as_raw_html()
        )

    def to_html(
        self,
        data: "pandas.DataFrame",
        schema: pandera.DataFrameSchema,
        error: Optional[pandera.errors.SchemaErrors] = None,
    ) -> str:
        error_segments = ""
        if error is None:
            report_dfs = self._create_success_report(data, schema)
            top_message = "✅ Data validation succeeded."
        else:
            report_dfs = self._create_error_report(data, schema, error)
            top_message = "❌ Data validation failed."

            error_segments = ""
            if report_dfs.schema_error_df is not None:
                error_segments += f"""
                <br>

                {self._format_schema_error_df(report_dfs.schema_error_df)}
                """

            if report_dfs.data_error_df is not None:
                error_segments += f"""
                <br>

                {self._format_data_error_df(report_dfs.data_error_df)}
                """

        return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Pandera Report</title>
            <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@3.4.1/dist/css/bootstrap.min.css" integrity="sha384-HSMxcRTRxnN+Bdg0JdbxYKrThecOKuH5zCYotlSAcp1+c8xmyTe9GYg1l9a69psu" crossorigin="anonymous">
            <style>
                .pandera-report {{
                    min-width: 60%;
                    margin: 0 auto;
                    padding: 20px;
                }}

                .report-title h1 {{
                    display: inline;
                    line-height: 60px;
                    padding-left: 10px;
                }}

                .report-title img {{
                    display: inline;
                    width: 60px;
                    float: left;
                }}

                table.gt_table {{
                    width: 100% !important;
                    margin-left: 0 !important;
                }}
            </style>
        </head>
        <body>
            <div class="pandera-report">
                <div class="report-title">
                    <img src="https://raw.githubusercontent.com/pandera-dev/pandera/main/docs/source/_static/pandera-logo.png" alt="Pandera Logo">
                    <h1>Pandera Report</h1>
                </div>

                <h3>{top_message}</h3>

                {self._format_summary_df(report_dfs.summary)}

                <br>

                {self._format_data_preview_df(report_dfs.data_preview)}

                {error_segments}
            </div>
        </body>
        </html>
        """
