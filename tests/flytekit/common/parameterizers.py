from datetime import timedelta
from itertools import product

from six.moves import range

import flytekit.models.core.types
from flytekit.common.types.impl import blobs as _blob_impl
from flytekit.common.types.impl import schema as _schema_impl
from flytekit.models.core import identifier, literals, interface, security
from flytekit.models.core import types as _core_types
from flytekit.models.core.compiler import CompiledTask as _compiledTask
from flytekit.models.admin import task as task
from flytekit.models.core.task import Resources as _task_resource, Container as _task_container

LIST_OF_SCALAR_LITERAL_TYPES = [
    flytekit.models.core.types.LiteralType(simple=flytekit.models.core.types.SimpleType.BINARY),
    flytekit.models.core.types.LiteralType(simple=flytekit.models.core.types.SimpleType.BOOLEAN),
    flytekit.models.core.types.LiteralType(simple=flytekit.models.core.types.SimpleType.DATETIME),
    flytekit.models.core.types.LiteralType(simple=flytekit.models.core.types.SimpleType.DURATION),
    flytekit.models.core.types.LiteralType(simple=flytekit.models.core.types.SimpleType.ERROR),
    flytekit.models.core.types.LiteralType(simple=flytekit.models.core.types.SimpleType.FLOAT),
    flytekit.models.core.types.LiteralType(simple=flytekit.models.core.types.SimpleType.INTEGER),
    flytekit.models.core.types.LiteralType(simple=flytekit.models.core.types.SimpleType.NONE),
    flytekit.models.core.types.LiteralType(simple=flytekit.models.core.types.SimpleType.STRING),
    flytekit.models.core.types.LiteralType(
        schema=flytekit.models.core.types.SchemaType(
            [
                flytekit.models.core.types.SchemaType.SchemaColumn("a", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.INTEGER),
                flytekit.models.core.types.SchemaType.SchemaColumn("b", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN),
                flytekit.models.core.types.SchemaType.SchemaColumn("c", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DATETIME),
                flytekit.models.core.types.SchemaType.SchemaColumn("d", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DURATION),
                flytekit.models.core.types.SchemaType.SchemaColumn("e", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.FLOAT),
                flytekit.models.core.types.SchemaType.SchemaColumn("f", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.STRING),
            ]
        )
    ),
    flytekit.models.core.types.LiteralType(
        blob=_core_types.BlobType(
            format="",
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )
    ),
    flytekit.models.core.types.LiteralType(
        blob=_core_types.BlobType(
            format="csv",
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )
    ),
    flytekit.models.core.types.LiteralType(
        blob=_core_types.BlobType(
            format="",
            dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,
        )
    ),
    flytekit.models.core.types.LiteralType(
        blob=_core_types.BlobType(
            format="csv",
            dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,
        )
    ),
]


LIST_OF_COLLECTION_LITERAL_TYPES = [
    flytekit.models.core.types.LiteralType(collection_type=literal_type) for literal_type in LIST_OF_SCALAR_LITERAL_TYPES
]

LIST_OF_NESTED_COLLECTION_LITERAL_TYPES = [
    flytekit.models.core.types.LiteralType(collection_type=literal_type) for literal_type in LIST_OF_COLLECTION_LITERAL_TYPES
]

LIST_OF_ALL_LITERAL_TYPES = (
    LIST_OF_SCALAR_LITERAL_TYPES + LIST_OF_COLLECTION_LITERAL_TYPES + LIST_OF_NESTED_COLLECTION_LITERAL_TYPES
)

LIST_OF_INTERFACES = [
    interface.TypedInterface(
        {"a": interface.Variable(t, "description 1")},
        {"b": interface.Variable(t, "description 2")},
    )
    for t in LIST_OF_ALL_LITERAL_TYPES
]


LIST_OF_RESOURCE_ENTRIES = [
    _task_resource.ResourceEntry(_task_resource.ResourceName.CPU, "1"),
    _task_resource.ResourceEntry(_task_resource.ResourceName.GPU, "1"),
    _task_resource.ResourceEntry(_task_resource.ResourceName.MEMORY, "1G"),
    _task_resource.ResourceEntry(_task_resource.ResourceName.STORAGE, "1G"),
    _task_resource.ResourceEntry(_task_resource.ResourceName.EPHEMERAL_STORAGE, "1G"),
]


LIST_OF_RESOURCE_ENTRY_LISTS = [LIST_OF_RESOURCE_ENTRIES]


LIST_OF_RESOURCES = [
    _task_resource(request, limit)
    for request, limit in product(LIST_OF_RESOURCE_ENTRY_LISTS, LIST_OF_RESOURCE_ENTRY_LISTS)
]


LIST_OF_RUNTIME_METADATA = [
    task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.OTHER, "1.0.0", "python"),
    task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0b0", "golang"),
]


LIST_OF_RETRY_POLICIES = [literals.RetryStrategy(retries=i) for i in [0, 1, 3, 100]]

LIST_OF_INTERRUPTIBLE = [None, True, False]

LIST_OF_TASK_METADATA = [
    task.TaskMetadata(
        discoverable,
        runtime_metadata,
        timeout,
        retry_strategy,
        interruptible,
        discovery_version,
        deprecated,
    )
    for discoverable, runtime_metadata, timeout, retry_strategy, interruptible, discovery_version, deprecated in product(
        [True, False],
        LIST_OF_RUNTIME_METADATA,
        [timedelta(days=i) for i in range(3)],
        LIST_OF_RETRY_POLICIES,
        LIST_OF_INTERRUPTIBLE,
        ["1.0"],
        ["deprecated"],
    )
]


LIST_OF_TASK_TEMPLATES = [
    task.TaskTemplate(
        identifier.Identifier(identifier.ResourceType.TASK, "project", "domain", "name", "version"),
        "python",
        task_metadata,
        interfaces,
        {"a": 1, "b": [1, 2, 3], "c": "abc", "d": {"x": 1, "y": 2, "z": 3}},
        container=_task_container(
            "my_image",
            ["this", "is", "a", "cmd"],
            ["this", "is", "an", "arg"],
            resources,
            {"a": "b"},
            {"d": "e"},
        ),
    )
    for task_metadata, interfaces, resources in product(LIST_OF_TASK_METADATA, LIST_OF_INTERFACES, LIST_OF_RESOURCES)
]

LIST_OF_CONTAINERS = [
    _task_container(
        "my_image",
        ["this", "is", "a", "cmd"],
        ["this", "is", "an", "arg"],
        resources,
        {"a": "b"},
        {"d": "e"},
    )
    for resources in LIST_OF_RESOURCES
]

LIST_OF_TASK_CLOSURES = [task.TaskClosure(_compiledTask(template)) for template in LIST_OF_TASK_TEMPLATES]

LIST_OF_SCALARS_AND_PYTHON_VALUES = [
    (literals.Scalar(primitive=literals.Primitive(integer=100)), 100),
    (literals.Scalar(primitive=literals.Primitive(float_value=500.0)), 500.0),
    (literals.Scalar(primitive=literals.Primitive(boolean=True)), True),
    (literals.Scalar(primitive=literals.Primitive(string_value="hello")), "hello"),
    (
        literals.Scalar(primitive=literals.Primitive(duration=timedelta(seconds=5))),
        timedelta(seconds=5),
    ),
    (literals.Scalar(none_type=literals.Void()), None),
    (
        literals.Scalar(
            blob=literals.Blob(
                literals.BlobMetadata(_core_types.BlobType("csv", _core_types.BlobType.BlobDimensionality.SINGLE)),
                "s3://some/where",
            )
        ),
        _blob_impl.Blob("s3://some/where", format="csv"),
    ),
    (
        literals.Scalar(
            blob=literals.Blob(
                literals.BlobMetadata(_core_types.BlobType("", _core_types.BlobType.BlobDimensionality.SINGLE)),
                "s3://some/where",
            )
        ),
        _blob_impl.Blob("s3://some/where"),
    ),
    (
        literals.Scalar(
            blob=literals.Blob(
                literals.BlobMetadata(_core_types.BlobType("csv", _core_types.BlobType.BlobDimensionality.MULTIPART)),
                "s3://some/where/",
            )
        ),
        _blob_impl.MultiPartBlob("s3://some/where/", format="csv"),
    ),
    (
        literals.Scalar(
            blob=literals.Blob(
                literals.BlobMetadata(_core_types.BlobType("", _core_types.BlobType.BlobDimensionality.MULTIPART)),
                "s3://some/where/",
            )
        ),
        _blob_impl.MultiPartBlob("s3://some/where/"),
    ),
    (
        literals.Scalar(
            schema=literals.Schema(
                "s3://some/where/",
                flytekit.models.core.types.SchemaType(
                    [
                        flytekit.models.core.types.SchemaType.SchemaColumn("a", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.INTEGER),
                        flytekit.models.core.types.SchemaType.SchemaColumn("b", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN),
                        flytekit.models.core.types.SchemaType.SchemaColumn("c", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DATETIME),
                        flytekit.models.core.types.SchemaType.SchemaColumn("d", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DURATION),
                        flytekit.models.core.types.SchemaType.SchemaColumn("e", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.FLOAT),
                        flytekit.models.core.types.SchemaType.SchemaColumn("f", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.STRING),
                    ]
                ),
            )
        ),
        _schema_impl.Schema(
            "s3://some/where/",
            _schema_impl.SchemaType.promote_from_model(
                flytekit.models.core.types.SchemaType(
                    [
                        flytekit.models.core.types.SchemaType.SchemaColumn("a", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.INTEGER),
                        flytekit.models.core.types.SchemaType.SchemaColumn("b", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN),
                        flytekit.models.core.types.SchemaType.SchemaColumn("c", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DATETIME),
                        flytekit.models.core.types.SchemaType.SchemaColumn("d", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DURATION),
                        flytekit.models.core.types.SchemaType.SchemaColumn("e", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.FLOAT),
                        flytekit.models.core.types.SchemaType.SchemaColumn("f", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.STRING),
                    ]
                )
            ),
        ),
    ),
]

LIST_OF_SCALAR_LITERALS_AND_PYTHON_VALUE = [
    (literals.Literal(scalar=s), v) for s, v in LIST_OF_SCALARS_AND_PYTHON_VALUES
]

LIST_OF_LITERAL_COLLECTIONS_AND_PYTHON_VALUE = [
    (literals.LiteralCollection(literals=[l, l, l]), [v, v, v]) for l, v in LIST_OF_SCALAR_LITERALS_AND_PYTHON_VALUE
]

LIST_OF_ALL_LITERALS_AND_VALUES = (
    LIST_OF_SCALAR_LITERALS_AND_PYTHON_VALUE + LIST_OF_LITERAL_COLLECTIONS_AND_PYTHON_VALUE
)

LIST_OF_SECRETS = [
    None,
    security.Secret(group="x", key="g"),
    security.Secret(group="x", key="y", mount_requirement=security.Secret.MountType.ANY),
    security.Secret(group="x", key="y", group_version="1", mount_requirement=security.Secret.MountType.FILE),
]

LIST_RUN_AS = [
    None,
    security.Identity(iam_role="role"),
    security.Identity(k8s_service_account="service_account"),
]

LIST_OF_SECURITY_CONTEXT = [
    security.SecurityContext(run_as=r, secrets=s, tokens=None) for r in LIST_RUN_AS for s in LIST_OF_SECRETS
] + [None]
