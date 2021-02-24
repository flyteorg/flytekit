import datetime
import os
import typing
from dataclasses import dataclass
from datetime import timedelta

import pandas
import pytest
from dataclasses_json import dataclass_json
from flyteidl.core import errors_pb2

import flytekit
from flytekit import ContainerTask, SQLTask, dynamic, kwtypes, maptask
from flytekit.common.translator import get_serializable
from flytekit.core import context_manager, launch_plan, promise
from flytekit.core.condition import conditional
from flytekit.core.context_manager import ExecutionState, FlyteContext, Image, ImageConfig
from flytekit.core.node import Node
from flytekit.core.promise import NodeOutput, Promise, VoidPromise
from flytekit.core.resources import Resources
from flytekit.core.task import TaskMetadata, task
from flytekit.core.testing import patch, task_mock
from flytekit.core.type_engine import (
    DictTransformer,
    ListTransformer,
    PathLikeTransformer,
    RestrictedTypeError,
    SimpleTransformer,
    TypeEngine,
)
from flytekit.core.workflow import workflow
from flytekit.interfaces.data.data_proxy import FileAccessProvider
from flytekit.models import types as model_types
from flytekit.models.core import types as _core_types
from flytekit.models.core.types import BlobType
from flytekit.models.interface import Parameter
from flytekit.models.literals import Blob, BlobMetadata, Literal, LiteralCollection, LiteralMap, Primitive, Scalar
from flytekit.models.task import Resources as _resource_models
from flytekit.models.types import LiteralType, SimpleType
from flytekit.types.file.file import FlyteFile
from flytekit.types.schema import FlyteSchema, SchemaFormat, SchemaOpenMode


def test_proto():
    @task
    def t1(in1: errors_pb2.ContainerError) -> errors_pb2.ContainerError:
        e2 = errors_pb2.ContainerError(code=in1.code, message=in1.message + "!!!", kind=in1.kind + 1)
        return e2

    @workflow
    def wf(a: errors_pb2.ContainerError) -> errors_pb2.ContainerError:
        return t1(in1=a)

    e1 = errors_pb2.ContainerError(code="test", message="hello world", kind=1)
    e_out = wf(a=e1)
    assert e_out.kind == 2
    assert e_out.message == "hello world!!!"


def test_serialize():
    serialization_settings = context_manager.SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
        env={},
    )
    sdk_lp = get_serializable(serialization_settings, lp)
