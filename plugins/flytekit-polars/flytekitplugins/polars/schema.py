import os
import typing
from typing import Type

import polars as pl

from flytekit import FlyteContext
from flytekit.extend import T, TypeEngine, TypeTransformer
from flytekit.models.literals import Literal, Scalar, Schema
from flytekit.models.types import LiteralType, SchemaType
from flytekit.types.schema import LocalIOSchemaReader, LocalIOSchemaWriter, SchemaEngine, SchemaFormat, SchemaHandler
from flytekit.types.schema.types import FlyteSchemaTransformer


class PolarsSchemaReader(LocalIOSchemaReader[pl.DataFrame]):
    """
    Implements how polars.DataFrame should be read using the ``open`` method of FlyteSchema
    """

    def __init__(
        self,
        from_path: str,
        cols: typing.Optional[typing.Dict[str, type]],
        fmt: SchemaFormat,
    ):
        super().__init__(from_path, cols, fmt)

    def all(self, **kwargs) -> pl.DataFrame:
        if self._fmt == SchemaFormat.PARQUET:
            return pl.read_parquet(self.from_path + "/00000")
        raise AssertionError("Only Parquet type files are supported for polars dataframe currently")


class PolarsSchemaWriter(LocalIOSchemaWriter[pl.DataFrame]):
    """
    Implements how polars.DataFrame should be written to using ``open`` method of FlyteSchema
    """

    def __init__(
        self,
        to_path: os.PathLike,
        cols: typing.Optional[typing.Dict[str, type]],
        fmt: SchemaFormat,
    ):
        super().__init__(str(to_path), cols, fmt)

    def write(self, *dfs: pl.DataFrame, **kwargs):
        if dfs is None or len(dfs) == 0:
            return
        if len(dfs) > 1:
            raise AssertionError("Only a single polars.DataFrame can be written per variable currently")
        if self._fmt == SchemaFormat.PARQUET:
            out_path = self.to_path + "/00000"
            df = dfs[0]

            # Polars 0.13.12 deprecated to_parquet in favor of write_parquet
            if hasattr(df, "write_parquet"):
                df.write_parquet(out_path)
            else:
                df.to_parquet(out_path)
            return
        raise AssertionError("Only Parquet type files are supported for polars dataframe currently")


class PolarsDataFrameTransformer(TypeTransformer[pl.DataFrame]):
    """
    Transforms polars.DataFrame to and from a Schema (typed/untyped)
    """

    _SUPPORTED_TYPES: typing.Dict[
        type, SchemaType.SchemaColumn.SchemaColumnType
    ] = FlyteSchemaTransformer._SUPPORTED_TYPES

    def __init__(self):
        super().__init__("polars-df-transformer", pl.DataFrame)

    @staticmethod
    def _get_schema_type() -> SchemaType:
        return SchemaType(columns=[])

    def get_literal_type(self, t: Type[pl.DataFrame]) -> LiteralType:
        return LiteralType(schema=self._get_schema_type())

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: pl.DataFrame,
        python_type: Type[pl.DataFrame],
        expected: LiteralType,
    ) -> Literal:
        local_dir = ctx.file_access.get_random_local_directory()
        w = PolarsSchemaWriter(to_path=local_dir, cols=None, fmt=SchemaFormat.PARQUET)
        w.write(python_val)
        remote_path = ctx.file_access.get_random_remote_directory()
        ctx.file_access.put_data(local_dir, remote_path, is_multipart=True)
        return Literal(scalar=Scalar(schema=Schema(remote_path, self._get_schema_type())))

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Type[pl.DataFrame],
    ) -> T:
        if not (lv and lv.scalar and lv.scalar.schema):
            return pl.DataFrame()
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.download_directory(lv.scalar.schema.uri, local_dir)
        r = PolarsSchemaReader(from_path=local_dir, cols=None, fmt=SchemaFormat.PARQUET)
        return r.all()


SchemaEngine.register_handler(
    SchemaHandler(
        "pl.Dataframe-Schema",
        pl.DataFrame,
        PolarsSchemaReader,
        PolarsSchemaWriter,
    )
)

TypeEngine.register(PolarsDataFrameTransformer())
