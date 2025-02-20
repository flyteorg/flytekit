from flytekit.core.python_function_task import EagerFailureHandlerTask
import re
import os
import typing
from collections import OrderedDict

import mock
import pytest

import flytekit.configuration
from flytekit import ContainerTask, ImageSpec, kwtypes
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.condition import conditional
from flytekit.core.python_auto_container import get_registerable_container_image
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.exceptions.user import FlyteAssertion, FlyteMissingTypeException
from flytekit.image_spec.image_spec import ImageBuildEngine
from flytekit.models.admin.workflow import WorkflowSpec
from flytekit.models.annotation import TypeAnnotation
from flytekit.models.literals import (
    BindingData,
    BindingDataCollection,
    BindingDataMap,
    Literal,
    Primitive,
    Scalar,
    Union,
    Void,
)
from flytekit.models.types import LiteralType, SimpleType, TypeStructure, UnionType
from flytekit.tools.translator import get_serializable
from flytekit.types.error.error import FlyteError

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


def test_failure():
    t = EagerFailureHandlerTask(inputs={"a": int})

    spec = get_serializable(OrderedDict(), serialization_settings, t)
    print(spec)

    assert spec.template.container.args == ['pyflyte-execute', '--inputs', '{{.input}}', '--output-prefix', '{{.outputPrefix}}', '--raw-output-data-prefix', '{{.rawOutputDataPrefix}}', '--checkpoint-path', '{{.checkpointOutputPrefix}}', '--prev-checkpoint', '{{.prevCheckpointPrefix}}', '--resolver', 'flytekit.core.python_function_task.eager_failure_task_resolver', '--', 'eager', 'failure', 'handler']


def test_loading():
    from flytekit.tools.module_loader import load_object_from_module

    resolver = load_object_from_module("flytekit.core.python_function_task.eager_failure_task_resolver")
    print(resolver)
    t = resolver.load_task([])
    assert isinstance(t, EagerFailureHandlerTask)
