import enum
import typing
import dataclasses
import datetime
import functools
import os
import random
import re
import sys
import tempfile
from typing_extensions import get_args
import typing
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum

import msgpack
import pytest
from dataclasses_json import DataClassJsonMixin
from google.protobuf.struct_pb2 import Struct
from typing_extensions import Annotated, get_origin

import flytekit
import flytekit.configuration
from flytekit import Secret, SQLTask, dynamic, kwtypes, map_task
from flytekit.configuration import FastSerializationSettings, Image, ImageConfig
from flytekit.core import context_manager, launch_plan, promise
from flytekit.core.condition import conditional
from flytekit.core.context_manager import ExecutionState
from flytekit.core.data_persistence import FileAccessProvider, flyte_tmp_dir
from flytekit.core.hash import HashMethod
from flytekit.core.node import Node
from flytekit.core.promise import NodeOutput, Promise, VoidPromise
from flytekit.core.resources import Resources
from flytekit.types.pickle.pickle import FlytePickleTransformer
from flytekit.core.task import TaskMetadata, task
from flytekit.core.testing import patch, task_mock
from flytekit.core.type_engine import RestrictedTypeError, SimpleTransformer, TypeEngine, TypeTransformerFailedError
from flytekit.core.workflow import workflow
from flytekit.exceptions.user import FlyteValidationException, FlyteFailureNodeInputMismatchException
from flytekit.models import literals as _literal_models
from flytekit.models.core import types as _core_types
from flytekit.models.interface import Parameter
from flytekit.models.literals import Binary
from flytekit.models.task import Resources as _resource_models
from flytekit.models.types import LiteralType, SimpleType
from flytekit.tools.translator import get_serializable
from flytekit.types.directory import FlyteDirectory, TensorboardLogs
from flytekit.types.error import FlyteError
from flytekit.types.file import FlyteFile
from flytekit.types.schema import FlyteSchema, SchemaOpenMode
from flytekit.types.structured.structured_dataset import StructuredDataset


from collections import OrderedDict

from flyteidl.core import identifier_pb2
from typing_extensions import Annotated
from enum import Enum, EnumMeta
from flytekit.configuration import Config, Image, ImageConfig, SerializationSettings
from flytekit.core.artifact import Artifact
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.remote.remote import FlyteRemote
from flytekit.tools.translator import get_serializable
from flytekit.types.structured.structured_dataset import StructuredDataset
from dataclasses import dataclass


serialization_settings = flytekit.configuration.SerializationSettings(
    project="proj",
    domain="dom",
    version="123",
    image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
    env={},
)


def test_jkljlk():
    r = FlyteRemote(
        Config.auto(config_file="/Users/ytong/.flyte/config-sandbox.yaml"),
        default_project="flytesnacks",
        default_domain="development",
    )
    lp = r.fetch_launch_plan(name="yt_dbg.scratchpad.union_enums.wf", version="m8zKMpKeywemTgH5LDwKFw")
    ex = r.execute(lp, inputs={"x": "one"})
    print(ex)


def test_fetch():
    class Color(Enum):
        RED = "one"
        GREEN = "two"
        BLUE = "blue"

    @dataclass
    class A:
        a: str = None

    ctx = FlyteContextManager.current_context()

    pt = typing.Union[Color, A]
    lt = TypeEngine.to_literal_type(pt)
    guessed = TypeEngine.guess_python_type(lt)
    print(guessed)
    guessed_enum = get_args(guessed)[0]
    print(guessed_enum)
    enum_val = guessed_enum("one")
    TypeEngine.to_literal(ctx, enum_val, guessed, lt)


