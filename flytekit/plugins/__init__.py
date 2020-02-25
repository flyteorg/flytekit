from __future__ import absolute_import
from flytekit.tools import lazy_loader as _lazy_loader


pyspark = _lazy_loader.lazy_load_module("pyspark")  # type: types.ModuleType

k8s = _lazy_loader.lazy_load_module("k8s")  # type: types.ModuleType
type(k8s).add_sub_module("io.api.core.v1.generated_pb2")
type(k8s).add_sub_module("io.apimachinery.pkg.api.resource.generated_pb2")

flyteidl = _lazy_loader.lazy_load_module("flyteidl")  # type: types.ModuleType
type(flyteidl).add_sub_module("plugins.sidecar_pb2")

numpy = _lazy_loader.lazy_load_module("numpy")  # type: types.ModuleType
pandas = _lazy_loader.lazy_load_module("pandas")  # type: types.ModuleType

hmsclient = _lazy_loader.lazy_load_module("hmsclient")  # type: types.ModuleType
type(hmsclient).add_sub_module("genthrift.hive_metastore.ttypes")

_lazy_loader.LazyLoadPlugin(
    "spark",
    ["pyspark>=2.4.0,<3.0.0"],
    [pyspark]
)

_lazy_loader.LazyLoadPlugin(
    "sidecar",
    ["k8s-proto>=0.0.2,<1.0.0"],
    [k8s, flyteidl]
)

_lazy_loader.LazyLoadPlugin(
    "schema",
    [
        "numpy>=1.14.0,<2.0.0",
        "pandas>=0.22.0,<2.0.0",
        "pyarrow>=0.11.0,<1.0.0",
    ],
    [numpy, pandas]
)

_lazy_loader.LazyLoadPlugin(
    "hive_sensor",
    [
        "hmsclient>=0.0.1,<1.0.0",
    ],
    [hmsclient]
)
