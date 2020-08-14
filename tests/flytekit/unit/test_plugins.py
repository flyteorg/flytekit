from __future__ import absolute_import

import pytest

from flytekit import plugins
from flytekit.tools import lazy_loader


@pytest.mark.run(order=0)
def test_spark_plugin():
    plugins.pyspark.SparkContext
    import pyspark

    assert plugins.pyspark.SparkContext == pyspark.SparkContext


@pytest.mark.run(order=1)
def test_schema_plugin():
    plugins.numpy.dtype
    plugins.pandas.DataFrame
    import numpy
    import pandas

    assert plugins.numpy.dtype == numpy.dtype
    assert pandas.DataFrame == pandas.DataFrame


@pytest.mark.run(order=2)
def test_sidecar_plugin():
    assert isinstance(
        plugins.k8s.io.api.core.v1.generated_pb2, lazy_loader._LazyLoadModule
    )
    assert isinstance(
        plugins.k8s.io.apimachinery.pkg.api.resource.generated_pb2,
        lazy_loader._LazyLoadModule,
    )
    import k8s.io.api.core.v1.generated_pb2
    import k8s.io.apimachinery.pkg.api.resource.generated_pb2

    k8s.io.api.core.v1.generated_pb2.Container
    k8s.io.apimachinery.pkg.api.resource.generated_pb2.Quantity


@pytest.mark.run(order=2)
def test_hive_sensor_plugin():
    assert isinstance(plugins.hmsclient, lazy_loader._LazyLoadModule)
    assert isinstance(
        plugins.hmsclient.genthrift.hive_metastore.ttypes, lazy_loader._LazyLoadModule
    )
    import hmsclient
    import hmsclient.genthrift.hive_metastore.ttypes

    hmsclient.HMSClient
    hmsclient.genthrift.hive_metastore.ttypes.NoSuchObjectException
