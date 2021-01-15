import pytest

from flytekit import old_plugins
from flytekit.tools import lazy_loader


@pytest.mark.run(order=0)
def test_spark_plugin():
    old_plugins.pyspark.SparkContext
    import pyspark

    assert old_plugins.pyspark.SparkContext == pyspark.SparkContext


@pytest.mark.run(order=1)
def test_schema_plugin():
    old_plugins.numpy.dtype
    old_plugins.pandas.DataFrame
    import numpy
    import pandas

    assert old_plugins.numpy.dtype == numpy.dtype
    assert pandas.DataFrame == pandas.DataFrame


@pytest.mark.run(order=2)
def test_sidecar_plugin():
    assert isinstance(old_plugins.k8s.io.api.core.v1.generated_pb2, lazy_loader._LazyLoadModule)
    assert isinstance(old_plugins.k8s.io.apimachinery.pkg.api.resource.generated_pb2, lazy_loader._LazyLoadModule, )
    import k8s.io.api.core.v1.generated_pb2
    import k8s.io.apimachinery.pkg.api.resource.generated_pb2

    k8s.io.api.core.v1.generated_pb2.Container
    k8s.io.apimachinery.pkg.api.resource.generated_pb2.Quantity


@pytest.mark.run(order=2)
def test_hive_sensor_plugin():
    assert isinstance(old_plugins.hmsclient, lazy_loader._LazyLoadModule)
    assert isinstance(old_plugins.hmsclient.genthrift.hive_metastore.ttypes, lazy_loader._LazyLoadModule)
    import hmsclient
    import hmsclient.genthrift.hive_metastore.ttypes

    hmsclient.HMSClient
    hmsclient.genthrift.hive_metastore.ttypes.NoSuchObjectException
