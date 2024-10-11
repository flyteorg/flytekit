import typing
from typing import Annotated

import pandas as pd
import pandera as pa
from flytekit import ImageSpec, task, workflow
from flytekitplugins.pandera import PanderaValidationConfig
from pandera.typing import DataFrame, Series



custom_image = ImageSpec(
    apt_packages=["git"],
    packages=[
        "git+https://github.com/flyteorg/flytekit@f8e7954da#subdirectory=plugins/flytekit-pandera",
        "scikit-learn",
        "pyarrow",
    ],
)


class InSchema(pa.DataFrameModel):
    hourly_pay: float = pa.Field(ge=7)
    hours_worked: float = pa.Field(ge=10)

    @pa.check("hourly_pay", "hours_worked")
    def check_numbers_are_positive(cls, series: Series) -> Series[bool]:
        """Defines a column-level custom check."""
        return series > 0

    class Config:
        coerce = True


class IntermediateSchema(InSchema):
    total_pay: float

    @pa.dataframe_check
    def check_total_pay(cls, df: DataFrame) -> Series[bool]:
        """Defines a dataframe-level custom check."""
        return df["total_pay"] == df["hourly_pay"] * df["hours_worked"]


class OutSchema(IntermediateSchema):
    worker_id: Series[str] = pa.Field()


pandera_conf = PanderaValidationConfig(on_error="report")


@task(container_image=custom_image, enable_deck=True, deck_fields=[])
def dict_to_dataframe(data: dict) -> Annotated[DataFrame[InSchema], pandera_conf]:
    """Helper task to convert a dictionary input to a dataframe."""
    return pd.DataFrame(data)


@task(container_image=custom_image, enable_deck=True, deck_fields=[])
def total_pay(df: Annotated[DataFrame[InSchema], pandera_conf]) -> Annotated[DataFrame[IntermediateSchema], pandera_conf]:  # noqa : F811
    return df.assign(total_pay=df.hourly_pay * df.hours_worked)


@task(container_image=custom_image, enable_deck=True, deck_fields=[])
def add_ids(
    df: Annotated[DataFrame[IntermediateSchema], pandera_conf],
    worker_ids: typing.List[str],
) -> Annotated[DataFrame[OutSchema], pandera_conf]:
    return df.assign(worker_id=worker_ids)


@workflow
def process_data(  # noqa : F811
    data: dict = {
        # "hourly_pay": [12.0, 13.5, 10.1],
        "hourly_pay": [1, 2, 3],
        "hours_worked": [30.5, 40.0, 41.75],
    },
    worker_ids: typing.List[str] = ["a", "b", "c"],
) -> DataFrame[OutSchema]:
    return add_ids(df=total_pay(df=dict_to_dataframe(data=data)), worker_ids=worker_ids)
