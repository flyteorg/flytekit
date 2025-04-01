from flytekit import task, workflow
import numpy as np
import dask.array as da
import xarray as xr


def _sample_dataset() -> xr.Dataset:
    return xr.Dataset(
        {"test": (("x", "y"), da.random.uniform(size=(32, 32)))},
    )


def test_xarray_zarr_dataarray_plugin():

    @task
    def _generate_xarray() -> xr.DataArray:
        return _sample_dataset()["test"]

    @task
    def _consume_xarray(ds: xr.DataArray) -> xr.DataArray:
        return ds

    @workflow
    def _xarray_wf() -> xr.DataArray:
        ds = _generate_xarray()
        return _consume_xarray(ds=ds)

    array = _xarray_wf()
    assert isinstance(array, xr.DataArray)


def test_xarray_zarr_dataset_plugin():

    @task
    def _generate_xarray() -> xr.Dataset:
        return _sample_dataset()

    @task
    def _consume_xarray(ds: xr.Dataset) -> xr.Dataset:
        return ds

    @workflow
    def _xarray_wf() -> xr.Dataset:
        ds = _generate_xarray()
        return _consume_xarray(ds=ds)

    array = _xarray_wf()
    assert isinstance(array, xr.Dataset)
