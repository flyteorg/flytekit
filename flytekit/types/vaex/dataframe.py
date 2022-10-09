import os
import typing
from typing import Type

import vaex

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import T, TypeEngine, TypeTransformer
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType
from flytekit.types.schema import LocalIOSchemaReader, LocalIOSchemaWriter, SchemaFormat


class VaexSchemaReader(LocalIOSchemaReader[vaex.DataFrame]):
    def __init__(self, local_dir: os.PathLike, cols: typing.Optional[typing.Dict[str, type]], fmt: SchemaFormat):
        super().__init__(local_dir, cols, fmt)

    def _read(self, path: os.PathLike, **kwargs) -> vaex.DataFrame:
        return vaex.open(path, **kwargs)


class VaexSchemaWriter(LocalIOSchemaWriter[vaex.DataFrame]):
    def __init__(self, local_dir: os.PathLike, cols: typing.Optional[typing.Dict[str, type]], fmt: SchemaFormat):
        super().__init__(local_dir, cols, fmt)

    def _write(self, df: T, path: os.PathLike, **kwargs):
        return df.export_parquet(path)


class VaexDataFrameTransformer(TypeTransformer[vaex.DataFrame]):
    """
    TypeTransformer that supports vaex.DataFrame as a native type.
    """

    VAEX_DF_FORMAT = "VaexDataFrame"

    def __init__(self):
        super().__init__(name="Vaex DataFrame", t=vaex.DataFrame)

    def get_literal_type(self, t: Type[vaex.DataFrame]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.VAEX_DF_FORMAT, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
            )
        )

    def to_literal(
        self, ctx: FlyteContext, python_val: vaex.DataFrame, python_type: Type[vaex.DataFrame], expected: LiteralType
    ) -> Literal:
        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.VAEX_DF_FORMAT, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
            )
        )
        local_dir = ctx.file_access.get_random_local_directory()
        # save vaex df to a file
        w = VaexSchemaWriter(local_dir=local_dir, cols=None, fmt=SchemaFormat.PARQUET)
        w.write(python_val)
        remote_path = ctx.file_access.get_random_remote_path(local_dir)
        ctx.file_access.put_data(local_dir, remote_path, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[vaex.DataFrame]
    ) -> vaex.DataFrame:
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.get_data(lv.scalar.schema.uri, local_dir, is_multipart=True)
        # load vaex df from a file
        r = VaexSchemaReader(local_dir=local_dir, cols=None, fmt=SchemaFormat.PARQUET)
        return r.all()

    def guess_python_type(self, literal_type: LiteralType) -> typing.Type[vaex.DataFrame]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.VAEX_DF_FORMAT
        ):
            return vaex.DataFrame

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(VaexDataFrameTransformer())
