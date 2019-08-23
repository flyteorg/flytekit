from __future__ import absolute_import
from flytekit.tools import lazy_loader as _lazy_loader

pyspark = _lazy_loader.LazyLoadPlugin(
    "spark",
    ["pyspark>=2.4.0,<3.0.0"],
    [
        "pyspark",
    ]
)

k8s = _lazy_loader.LazyLoadPlugin(
    "sidecar",
    ["k8s-proto>=0.0.2,<1.0.0"],
    [
        "k8s.io.api.core.v1.generated_pb2",
        "k8s.io.apimachinery.pkg.api.resource.generated_pb2",
        "flyteidl.plugins.sidecar_pb2",
    ]
)

schema = _lazy_loader.LazyLoadPlugin(
    "schema",
    [
        "numpy>=1.14.0,<2.0.0",
        "pandas>=0.22.0,<1.0.0",
        "pyarrow>=0.11.0,<1.0.0",
    ],
    [
        "numpy",
        "pandas",
    ]
)
