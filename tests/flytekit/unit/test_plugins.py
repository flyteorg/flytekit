from __future__ import absolute_import
from lazy_import import LazyModule
import flytekit  # noqa
import pytest


@pytest.mark.run(order=0)
def test_spark_plugin():
    import pyspark
    assert isinstance(pyspark, LazyModule)
    pyspark.SparkContext


@pytest.mark.run(order=1)
def test_schema_plugin():
    import numpy
    import pandas
    assert isinstance(numpy, LazyModule)
    assert isinstance(pandas, LazyModule)
    numpy.dtype
    pandas.DataFrame()


@pytest.mark.run(order=2)
def test_sidecar_plugin():
    import k8s.io.api.core.v1.generated_pb2
    import k8s.io.apimachinery.pkg.api.resource.generated_pb2
    assert isinstance(k8s.io.api.core.v1.generated_pb2, LazyModule)
    assert isinstance(k8s.io.apimachinery.pkg.api.resource.generated_pb2, LazyModule)
    k8s.io.api.core.v1.generated_pb2.Container
    k8s.io.apimachinery.pkg.api.resource.generated_pb2.Quantity
