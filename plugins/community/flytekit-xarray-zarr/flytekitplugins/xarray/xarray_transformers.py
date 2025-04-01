import typing

import dask.distributed as dd

import xarray as xr
from flytekit import (
    Blob,
    BlobMetadata,
    BlobType,
    FlyteContext,
    Literal,
    LiteralType,
    Scalar,
)
from flytekit.extend import TypeEngine, TypeTransformer


class XarrayZarrTypeTransformer(TypeTransformer[xr.Dataset]):
    _TYPE_INFO = BlobType(format="binary", dimensionality=BlobType.BlobDimensionality.MULTIPART)

    def __init__(self) -> None:
        super().__init__(name="Xarray Dataset", t=xr.Dataset)

    def get_literal_type(self, t: typing.Type[xr.Dataset]) -> LiteralType:
        return LiteralType(blob=self._TYPE_INFO)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: xr.Dataset,
        python_type: typing.Type[xr.Dataset],
        expected: LiteralType,
    ) -> Literal:
        remote_dir = ctx.file_access.get_random_remote_path("data.zarr")
        # Opening with the dask client will attach the client eliminating the
        # need for users to connect to the client if a task tasks a xr.Dataset
        # type.
        with dd.Client(timeout=120):
            python_val.to_zarr(remote_dir, mode="w")
        return Literal(scalar=Scalar(blob=Blob(uri=remote_dir, metadata=BlobMetadata(type=self._TYPE_INFO))))

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: typing.Type[xr.Dataset],
    ) -> xr.Dataset:
        return xr.open_zarr(lv.scalar.blob.uri)

    def to_html(
        self,
        ctx: FlyteContext,
        python_val: xr.Dataset,
        expected_python_type: LiteralType,
    ) -> str:
        return python_val._repr_html_()


class XarrayDaZarrTypeTransformer(TypeTransformer[xr.DataArray]):
    _TYPE_INFO = BlobType(format="binary", dimensionality=BlobType.BlobDimensionality.MULTIPART)

    def __init__(self) -> None:
        super().__init__(name="Xarray DataArray", t=xr.DataArray)

    def get_literal_type(self, t: typing.Type[xr.DataArray]) -> LiteralType:
        return LiteralType(blob=self._TYPE_INFO)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: xr.DataArray,
        python_type: typing.Type[xr.DataArray],
        expected: LiteralType,
    ) -> Literal:
        remote_dir = ctx.file_access.get_random_remote_path("data.zarr")
        # Opening with the dask client will attach the client eliminating the
        # need for users to connect to the client if a task tasks a xr.Dataset
        # type.
        with dd.Client(timeout=120):
            python_val.to_zarr(remote_dir, mode="w")
        return Literal(scalar=Scalar(blob=Blob(uri=remote_dir, metadata=BlobMetadata(type=self._TYPE_INFO))))

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: typing.Type[xr.DataArray],
    ) -> xr.DataArray:
        # xr.open_zarr always opens a dataset, so we take the first variable
        return list(xr.open_zarr(lv.scalar.blob.uri).data_vars.values())[0]

    def to_html(
        self,
        ctx: FlyteContext,
        python_val: xr.DataArray,
        expected_python_type: LiteralType,
    ) -> str:
        return python_val._repr_html_()


TypeEngine.register(XarrayZarrTypeTransformer())
TypeEngine.register(XarrayDaZarrTypeTransformer())
