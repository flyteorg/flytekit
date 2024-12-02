from dataclasses import dataclass
from datetime import datetime
import os
import re
import textwrap
import time
import typing
from collections import OrderedDict
import uuid

import fsspec
import mock
import pytest
from flyteidl.core.errors_pb2 import ErrorDocument
from flyteidl.core import literals_pb2
from flyteidl.core.literals_pb2 import Literal, LiteralCollection, Scalar, Primitive
from google.protobuf.timestamp_pb2 import Timestamp


from flytekit.bin.entrypoint import _dispatch_execute, get_container_error_timestamp, normalize_inputs, setup_execution, get_traceback_str
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core import mock_stats
from flytekit.core.array_node_map_task import ArrayNodeMapTask
from flytekit.core.hash import HashMethod
from flytekit.models.core import identifier as id_models
from flytekit.core import context_manager
from flytekit.core.base_task import IgnoreOutputs
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.promise import VoidPromise
from flytekit.core.task import task
from flytekit.core.type_engine import TypeEngine, DataclassTransformer
from flytekit.exceptions import user as user_exceptions
from flytekit.exceptions.base import FlyteException
from flytekit.exceptions.scopes import system_entry_point
from flytekit.exceptions.user import FlyteRecoverableException, FlyteUserRuntimeException
from flytekit.models import literals as _literal_models
from flytekit.models.core import errors as error_models, execution
from flytekit.models.core import execution as execution_models
from flytekit.core.utils import write_proto_to_file
from flytekit.models.types import LiteralType, SimpleType


@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_dispatch_execute_void(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        python_task = mock.MagicMock()
        python_task.dispatch_execute.return_value = VoidPromise("testing")

        empty_literal_map = _literal_models.LiteralMap({}).to_flyte_idl()
        mock_load_proto.return_value = empty_literal_map

        def verify_output(*args, **kwargs):
            assert args[0] == empty_literal_map

        mock_write_to_file.side_effect = verify_output
        _dispatch_execute(ctx, lambda: python_task, "inputs path", "outputs prefix")
        assert mock_write_to_file.call_count == 1


@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_dispatch_execute_ignore(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True
    ctx = context_manager.FlyteContext.current_context()

    # IgnoreOutputs
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        python_task = mock.MagicMock()
        python_task.dispatch_execute.side_effect = FlyteUserRuntimeException(IgnoreOutputs())

        empty_literal_map = _literal_models.LiteralMap({}).to_flyte_idl()
        mock_load_proto.return_value = empty_literal_map

        # The system_entry_point decorator does different thing based on whether or not it's the
        # first time it's called. Using it here to mimic the fact that _dispatch_execute is
        # called by _execute_task, which also has a system_entry_point
        system_entry_point(_dispatch_execute)(ctx, lambda: python_task, "inputs path", "outputs prefix")
        assert mock_write_to_file.call_count == 0


@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_dispatch_execute_exception(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        python_task = mock.MagicMock()
        python_task.dispatch_execute.side_effect = Exception("random")

        empty_literal_map = _literal_models.LiteralMap({}).to_flyte_idl()
        mock_load_proto.return_value = empty_literal_map

        def verify_output(*args, **kwargs):
            assert isinstance(args[0], ErrorDocument)
            assert args[1].endswith("error.pb")

        mock_write_to_file.side_effect = verify_output
        _dispatch_execute(ctx, lambda: python_task, "inputs path", "outputs prefix")
        assert mock_write_to_file.call_count == 1

@pytest.mark.parametrize(
    "exception_value",
    [
        FlyteException("exception", timestamp=1),
        FlyteException("exception"),
        Exception("exception"),
    ]
)
@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_dispatch_execute_exception_with_multi_error_files(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto, exception_value: Exception, monkeypatch):
    monkeypatch.setenv("_F_DES", "1")
    monkeypatch.setenv("_F_WN", "worker")

    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        python_task = mock.MagicMock()
        python_task.dispatch_execute.side_effect = FlyteUserRuntimeException(exception_value)

        empty_literal_map = _literal_models.LiteralMap({}).to_flyte_idl()
        mock_load_proto.return_value = empty_literal_map

        def verify_output(*args, **kwargs):
            assert isinstance(args[0], ErrorDocument)
            container_error = args[0].error
            assert container_error.timestamp.seconds > 0
            assert container_error.worker == "worker"
            error_file_path = args[1]
            error_filename_base, error_filename_ext = os.path.splitext(os.path.split(error_file_path)[1])
            assert error_filename_base.startswith("error-")
            uuid.UUID(hex=error_filename_base[6:], version=4)
            assert error_filename_ext == ".pb"

        mock_write_to_file.side_effect = verify_output
        _dispatch_execute(ctx, lambda: python_task, "inputs path", "outputs prefix")
        assert mock_write_to_file.call_count == 1


@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_dispatch_execute_load_task_exception(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        def load_task():
            raise ModuleNotFoundError("Can not found module")

        empty_literal_map = _literal_models.LiteralMap({}).to_flyte_idl()
        mock_load_proto.return_value = empty_literal_map

        def verify_output(*args, **kwargs):
            assert isinstance(args[0], ErrorDocument)

        mock_write_to_file.side_effect = verify_output
        _dispatch_execute(ctx, load_task, "inputs path", "outputs prefix")
        assert mock_write_to_file.call_count == 1



@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_dispatch_execute_return_error_code(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        python_task = mock.MagicMock()
        python_task.dispatch_execute.side_effect = Exception("random")

        empty_literal_map = _literal_models.LiteralMap({}).to_flyte_idl()
        mock_load_proto.return_value = empty_literal_map

        def verify_output(*args, **kwargs):
            assert isinstance(args[0], ErrorDocument)

        mock_write_to_file.side_effect = verify_output

        with mock.patch.dict(os.environ, {"FLYTE_FAIL_ON_ERROR": "True"}):
            with pytest.raises(SystemExit):
                _dispatch_execute(ctx, lambda: python_task, "inputs path", "outputs prefix")


# This function collects outputs instead of writing them to a file.
# See flytekit.core.utils.write_proto_to_file for the original
def get_output_collector(results: OrderedDict):
    def output_collector(proto, path):
        results[path] = proto

    return output_collector


@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_dispatch_execute_normal(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    @task
    def t1(a: int) -> str:
        return f"string is: {a}"

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        input_literal_map = TypeEngine.dict_to_literal_map(ctx, {"a": 5})
        mock_load_proto.return_value = input_literal_map.to_flyte_idl()

        files = OrderedDict()
        mock_write_to_file.side_effect = get_output_collector(files)
        # See comment in test_dispatch_execute_ignore for why we need to decorate
        system_entry_point(_dispatch_execute)(ctx, lambda: t1, "inputs path", "outputs prefix")
        assert len(files) == 1

        # A successful run should've written an outputs file.
        k = list(files.keys())[0]
        assert "outputs.pb" in k

        v = list(files.values())[0]
        lm = _literal_models.LiteralMap.from_flyte_idl(v)
        assert lm.literals["o0"].scalar.primitive.string_value == "string is: 5"


@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_dispatch_execute_user_error_non_recov(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    @task
    def t1(a: int) -> str:
        # Should be interpreted as a non-recoverable user error
        raise ValueError(f"some exception {a}")
        return "hello"

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        input_literal_map = TypeEngine.dict_to_literal_map(ctx, {"a": 5})
        mock_load_proto.return_value = input_literal_map.to_flyte_idl()

        files = OrderedDict()
        mock_write_to_file.side_effect = get_output_collector(files)
        # See comment in test_dispatch_execute_ignore for why we need to decorate
        system_entry_point(_dispatch_execute)(ctx, lambda: t1, "inputs path", "outputs prefix")
        assert len(files) == 1

        # Exception should've caused an error file
        k = list(files.keys())[0]
        assert "error.pb" in k

        v = list(files.values())[0]
        ed = error_models.ErrorDocument.from_flyte_idl(v)
        assert ed.error.kind == error_models.ContainerError.Kind.NON_RECOVERABLE
        assert "some exception 5" in ed.error.message
        assert ed.error.origin == execution_models.ExecutionError.ErrorKind.USER


@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_dispatch_execute_user_error_recoverable(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    @task
    def t1(a: int) -> str:
        return f"A is {a}"

    @dynamic
    def my_subwf(a: int) -> typing.List[str]:
        # This also tests the dynamic/compile path
        raise user_exceptions.FlyteRecoverableException(f"recoverable {a}")
        return ["1", "2"]

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        input_literal_map = TypeEngine.dict_to_literal_map(ctx, {"a": 5})
        mock_load_proto.return_value = input_literal_map.to_flyte_idl()

        files = OrderedDict()
        mock_write_to_file.side_effect = get_output_collector(files)
        # See comment in test_dispatch_execute_ignore for why we need to decorate
        system_entry_point(_dispatch_execute)(ctx, lambda: my_subwf, "inputs path", "outputs prefix")
        assert len(files) == 1

        # Exception should've caused an error file
        k = list(files.keys())[0]
        assert "error.pb" in k

        v = list(files.values())[0]
        ed = error_models.ErrorDocument.from_flyte_idl(v)
        assert ed.error.kind == error_models.ContainerError.Kind.RECOVERABLE
        assert "recoverable 5" in ed.error.message
        assert ed.error.origin == execution_models.ExecutionError.ErrorKind.USER


@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_dispatch_execute_system_error(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        input_literal_map = TypeEngine.dict_to_literal_map(ctx, {"a": 5})
        mock_load_proto.return_value = input_literal_map.to_flyte_idl()

        python_task = mock.MagicMock()
        python_task.dispatch_execute.side_effect = Exception("some system exception")

        files = OrderedDict()
        mock_write_to_file.side_effect = get_output_collector(files)
        # See comment in test_dispatch_execute_ignore for why we need to decorate
        system_entry_point(_dispatch_execute)(ctx, lambda: python_task, "inputs path", "outputs prefix")
        assert len(files) == 1

        # Exception should've caused an error file
        k = list(files.keys())[0]
        assert "error.pb" in k

        v = list(files.values())[0]
        ed = error_models.ErrorDocument.from_flyte_idl(v)
        # System errors default to recoverable
        assert ed.error.kind == error_models.ContainerError.Kind.RECOVERABLE
        assert "some system exception" in ed.error.message
        assert ed.error.origin == execution_models.ExecutionError.ErrorKind.SYSTEM


def test_setup_disk_prefix():
    with setup_execution("qwerty") as ctx:
        assert isinstance(ctx.file_access._default_remote, fsspec.AbstractFileSystem)
        assert ctx.file_access._default_remote.protocol == "file" or set(ctx.file_access._default_remote.protocol) == {
            "file",
            "local",
        }


def test_setup_for_fast_register():
    dynamic_addl_distro = "distro"
    dynamic_dest_dir = "/root"
    with setup_execution(raw_output_data_prefix="qwerty", dynamic_addl_distro=dynamic_addl_distro, dynamic_dest_dir=dynamic_dest_dir) as ctx:
        assert ctx.serialization_settings.fast_serialization_settings.enabled is True
        assert ctx.serialization_settings.fast_serialization_settings.distribution_location == dynamic_addl_distro
        assert ctx.serialization_settings.fast_serialization_settings.destination_dir == dynamic_dest_dir


@mock.patch("google.auth.compute_engine._metadata")
def test_setup_cloud_prefix(mock_gcs):
    with setup_execution("s3://", checkpoint_path=None, prev_checkpoint=None) as ctx:
        assert ctx.file_access._default_remote.protocol[0] == "s3"

    with setup_execution("gs://", checkpoint_path=None, prev_checkpoint=None) as ctx:
        assert "gs" in ctx.file_access._default_remote.protocol


@mock.patch("google.auth.compute_engine._metadata")  # to prevent network calls
def test_persist_ss(mock_gcs):
    default_img = Image(name="default", fqn="test", tag="tag")
    ss = SerializationSettings(
        project="proj1",
        domain="dom",
        version="version123",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    ss_txt = ss.serialized_context
    os.environ["_F_SS_C"] = ss_txt
    with setup_execution("s3://", checkpoint_path=None, prev_checkpoint=None) as ctx:
        assert ctx.serialization_settings.project == "proj1"
        assert ctx.serialization_settings.domain == "dom"


def test_normalize_inputs():
    assert normalize_inputs("{{.rawOutputDataPrefix}}", "{{.checkpointOutputPrefix}}", "{{.prevCheckpointPrefix}}") == (
        None,
        None,
        None,
    )
    assert normalize_inputs("/raw", "/cp1", '""') == ("/raw", "/cp1", None)
    assert normalize_inputs("/raw", "/cp1", "") == ("/raw", "/cp1", None)
    assert normalize_inputs("/raw", "/cp1", "/prev") == ("/raw", "/cp1", "/prev")


@mock.patch("flytekit.bin.entrypoint.os")
def test_env_reading(mock_os):
    mock_env = {
        "FLYTE_INTERNAL_EXECUTION_PROJECT": "exec_proj",
        "FLYTE_INTERNAL_EXECUTION_DOMAIN": "exec_dom",
        "FLYTE_INTERNAL_EXECUTION_ID": "exec_name",
        "FLYTE_INTERNAL_TASK_PROJECT": "task_proj",
        "FLYTE_INTERNAL_TASK_DOMAIN": "task_dom",
        "FLYTE_INTERNAL_TASK_NAME": "task_name",
        "FLYTE_INTERNAL_TASK_VERSION": "task_ver",
    }
    mock_os.environ = mock_env

    with setup_execution("qwerty") as ctx:
        assert ctx.execution_state.user_space_params.task_id.name == "task_name"
        assert ctx.execution_state.user_space_params.task_id.version == "task_ver"
        assert ctx.execution_state.user_space_params.execution_id.name == "exec_name"


def test_get_traceback_str():
    try:
        try:
            try:
                raise RuntimeError("a")
            except Exception as e:
                raise FlyteRecoverableException("b") from e
        except Exception as e:
            # This is how Flyte wraps user exceptions
            raise FlyteUserRuntimeException(e) from e
    except Exception as e:
        traceback_str = get_traceback_str(e)

    expected_error_pattern = textwrap.dedent(r"""
        Trace:

            Traceback \(most recent call last\):
              File ".*test_python_entrypoint.py", line \d+, in test_get_traceback_str
                raise RuntimeError\("a"\)
            RuntimeError: a

            The above exception was the direct cause of the following exception:

            Traceback \(most recent call last\):
              File ".*test_python_entrypoint.py", line \d+, in test_get_traceback_str
                raise FlyteRecoverableException\("b"\) from e
            flytekit.exceptions.user.FlyteRecoverableException: USER:Recoverable: error=b, cause=a

        Message:

            FlyteRecoverableException: USER:Recoverable: error=b, cause=a""")
    # remove the initial newline
    expected_error_pattern = expected_error_pattern[1:]

    expected_error_re = re.compile(expected_error_pattern)
    print(traceback_str)  # helpful for debugging
    assert expected_error_re.match(traceback_str) is not None


def test_get_container_error_timestamp(monkeypatch) -> None:
    # Set the timezone to UTC
    monkeypatch.setenv("TZ", "UTC")
    if hasattr(time, 'tzset'):
        time.tzset()

    assert get_container_error_timestamp(FlyteException("foo", timestamp=10.5)) == Timestamp(seconds=10, nanos=500000000)

    current_dtime = datetime.now()
    error_timestamp = get_container_error_timestamp(RuntimeError("foo"))
    assert error_timestamp.ToDatetime() >= current_dtime

    current_dtime = datetime.now()
    error_timestamp = get_container_error_timestamp(FlyteException("foo"))
    assert error_timestamp.ToDatetime() >= current_dtime

    current_dtime = datetime.now()
    error_timestamp = get_container_error_timestamp(None)
    assert error_timestamp.ToDatetime() >= current_dtime


def get_flyte_context(tmp_path_factory, outputs_path):
    """
    This is a helper function to create a flyte context with the right parameters for testing offloading of literals.
    """
    ctx = context_manager.FlyteContext.current_context()
    return context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(
                engine_dir=tmp_path_factory.mktemp("engine_dir"),
                mode=context_manager.ExecutionState.Mode.TASK_EXECUTION,
                user_space_params=context_manager.ExecutionParameters(
                    execution_date=datetime.now(),
                    tmp_dir="/tmp",
                    stats=mock_stats.MockStats(),
                    logging=None,
                    raw_output_prefix="",
                    output_metadata_prefix=str(outputs_path.absolute()),
                    execution_id=id_models.WorkflowExecutionIdentifier("p", "d", "n"),
                ),
            ),
        ),
    )


def test_dispatch_execute_offloaded_literals(tmp_path_factory):
    @task
    def t1(a: typing.List[int]) -> typing.List[str]:
        return [f"string is: {x}" for x in a]

    inputs_path = tmp_path_factory.mktemp("inputs")
    outputs_path = tmp_path_factory.mktemp("outputs")

    ctx = context_manager.FlyteContext.current_context()
    with get_flyte_context(tmp_path_factory, outputs_path) as ctx:
        xs: typing.List[int] = [1, 2, 3]
        input_literal_map = _literal_models.LiteralMap(
            {
                "a": TypeEngine.to_literal(ctx, xs, typing.List[int], TypeEngine.to_literal_type(typing.List[int])),
            }
        )

        write_proto_to_file(input_literal_map.to_flyte_idl(), str(inputs_path/"inputs.pb"))

        with mock.patch.dict(os.environ, {"_F_L_MIN_SIZE_MB": "0"}):
            _dispatch_execute(ctx, lambda: t1, str(inputs_path/"inputs.pb"), str(outputs_path.absolute()))

            assert "error.pb" not in os.listdir(outputs_path)

            for ff in os.listdir(outputs_path):
                with open(outputs_path/ff, "rb") as f:
                    if ff == "outputs.pb":
                        lit = literals_pb2.LiteralMap()
                        lit.ParseFromString(f.read())
                        assert len(lit.literals) == 1
                        assert "o0" in lit.literals
                        assert lit.literals["o0"].HasField("offloaded_metadata") == True
                        assert lit.literals["o0"].offloaded_metadata.size_bytes == 62
                        assert lit.literals["o0"].offloaded_metadata.uri.endswith("/o0_offloaded_metadata.pb")
                        assert lit.literals["o0"].offloaded_metadata.inferred_type == LiteralType(collection_type=LiteralType(simple=SimpleType.STRING)).to_flyte_idl()
                    elif ff == "o0_offloaded_metadata.pb":
                        lit = literals_pb2.Literal()
                        lit.ParseFromString(f.read())
                        assert lit == Literal(
                            collection=LiteralCollection(
                                literals=[
                                    Literal(
                                        scalar=Scalar(primitive=Primitive(string_value="string is: 1")),
                                    ),
                                    Literal(
                                        scalar=Scalar(primitive=Primitive(string_value="string is: 2")),
                                    ),
                                    Literal(
                                        scalar=Scalar(primitive=Primitive(string_value="string is: 3")),
                                    ),
                                ]
                            )
                        )
                    else:
                        assert False, f"Unexpected file {ff}"


def test_dispatch_execute_offloaded_literals_two_outputs_offloaded(tmp_path_factory):
    @task
    def t1(xs: typing.List[int]) -> typing.Tuple[int, typing.List[str]]:
        return sum(xs), [f"string is: {x}" for x in xs]

    inputs_path = tmp_path_factory.mktemp("inputs")
    outputs_path = tmp_path_factory.mktemp("outputs")

    ctx = context_manager.FlyteContext.current_context()
    with get_flyte_context(tmp_path_factory, outputs_path) as ctx:
        xs: typing.List[int] = [1, 2, 3, 4]
        input_literal_map = _literal_models.LiteralMap(
            {
                "xs": _literal_models.Literal(
                    collection=_literal_models.LiteralCollection(
                        literals=[
                            _literal_models.Literal(
                                scalar=_literal_models.Scalar(primitive=_literal_models.Primitive(integer=x)),
                            ) for x in xs
                        ]
                    )
                )
            }
        )

        write_proto_to_file(input_literal_map.to_flyte_idl(), str(inputs_path/"inputs.pb"))

        with mock.patch.dict(os.environ, {"_F_L_MIN_SIZE_MB": "0"}):
            _dispatch_execute(ctx, lambda: t1, str(inputs_path/"inputs.pb"), str(outputs_path.absolute()))

            assert "error.pb" not in os.listdir(outputs_path)

            for ff in os.listdir(outputs_path):
                with open(outputs_path/ff, "rb") as f:
                    if ff == "outputs.pb":
                        lit = literals_pb2.LiteralMap()
                        lit.ParseFromString(f.read())
                        assert len(lit.literals) == 2
                        assert "o0" in lit.literals
                        assert lit.literals["o0"].HasField("offloaded_metadata") == True
                        assert lit.literals["o0"].offloaded_metadata.size_bytes == 6
                        assert lit.literals["o0"].offloaded_metadata.uri.endswith("/o0_offloaded_metadata.pb")
                        assert lit.literals["o0"].offloaded_metadata.inferred_type == LiteralType(simple=SimpleType.INTEGER).to_flyte_idl()
                        assert "o1" in lit.literals
                        assert lit.literals["o1"].HasField("offloaded_metadata") == True
                        assert lit.literals["o1"].offloaded_metadata.size_bytes == 82
                        assert lit.literals["o1"].offloaded_metadata.uri.endswith("/o1_offloaded_metadata.pb")
                        assert lit.literals["o1"].offloaded_metadata.inferred_type == LiteralType(collection_type=LiteralType(simple=SimpleType.STRING)).to_flyte_idl()
                    elif ff == "o0_offloaded_metadata.pb":
                        lit = literals_pb2.Literal()
                        lit.ParseFromString(f.read())
                        assert lit == Literal(
                            scalar=Scalar(primitive=Primitive(integer=10)),
                        )
                    elif ff == "o1_offloaded_metadata.pb":
                        lit = literals_pb2.Literal()
                        lit.ParseFromString(f.read())
                        assert lit == Literal(
                            collection=LiteralCollection(
                                literals=[
                                    Literal(
                                        scalar=Scalar(primitive=Primitive(string_value="string is: 1")),
                                    ),
                                    Literal(
                                        scalar=Scalar(primitive=Primitive(string_value="string is: 2")),
                                    ),
                                    Literal(
                                        scalar=Scalar(primitive=Primitive(string_value="string is: 3")),
                                    ),
                                    Literal(
                                        scalar=Scalar(primitive=Primitive(string_value="string is: 4")),
                                    ),
                                ]
                            )
                        )
                    else:
                        assert False, f"Unexpected file {ff}"


def test_dispatch_execute_offloaded_literals_two_outputs_only_second_one_offloaded(tmp_path_factory):
    @dataclass
    class DC:
        a: typing.List[int]
        b: typing.List[str]

    @task
    def t1(n: int) -> typing.Tuple[int, DC]:
        return n, DC(a=list(range(n)), b=[f"string is: {x}" for x in range(n)])

    inputs_path = tmp_path_factory.mktemp("inputs")
    outputs_path = tmp_path_factory.mktemp("outputs")

    with get_flyte_context(tmp_path_factory, outputs_path) as ctx:
        input_literal_map = _literal_models.LiteralMap(
            {
                "n": _literal_models.Literal(
                    scalar=_literal_models.Scalar(primitive=_literal_models.Primitive(integer=56_000)),
                )
            }
        )

        write_proto_to_file(input_literal_map.to_flyte_idl(), str(inputs_path/"inputs.pb"))

        # Notice how the threshold is set to 1MB
        with mock.patch.dict(os.environ, {"_F_L_MIN_SIZE_MB": "1"}):
            _dispatch_execute(ctx, lambda: t1, str(inputs_path/"inputs.pb"), str(outputs_path.absolute()))

            assert "error.pb" not in os.listdir(outputs_path)

            # o0 is not offloaded
            assert "o0_offloaded_metadata.pb" not in os.listdir(outputs_path)

            for ff in os.listdir(outputs_path):
                with open(outputs_path/ff, "rb") as f:
                    if ff == "outputs.pb":
                        lit = literals_pb2.LiteralMap()
                        lit.ParseFromString(f.read())
                        assert len(lit.literals) == 2

                        # o0 is not offloaded
                        assert "o0" in lit.literals
                        assert lit.literals["o0"].HasField("offloaded_metadata") is False
                        assert lit.literals["o0"].hash == ""

                        # o1 is offloaded
                        assert "o1" in lit.literals
                        assert lit.literals["o1"].HasField("offloaded_metadata") is True
                        assert lit.literals["o1"].offloaded_metadata.size_bytes == 1108538
                        assert lit.literals["o1"].offloaded_metadata.uri.endswith("/o1_offloaded_metadata.pb")
                        assert lit.literals["o1"].hash == "VS9bthLslGa8tjuVBCcmO3UdGHrkpyOBXzJlmY47fw8="
                        assert lit.literals["o1"].offloaded_metadata.inferred_type == DataclassTransformer().get_literal_type(DC).to_flyte_idl()
                    elif ff == "o1_offloaded_metadata.pb":
                        lit = literals_pb2.Literal()
                        lit.ParseFromString(f.read())
                        assert lit.hash == ""
                        # Load the dataclass from the proto
                        transformer = TypeEngine.get_transformer(DC)
                        dc = transformer.to_python_value(ctx, _literal_models.Literal.from_flyte_idl(lit), DC)
                        assert dc.a == list(range(56_000))
                    else:
                        assert False, f"Unexpected file {ff}"



def test_dispatch_execute_offloaded_literals_annotated_hash(tmp_path_factory):
    class A:
        def __init__(self, a: int):
            self.a = a

    @task
    def t1(n: int) -> typing.Annotated[A, HashMethod(lambda x: str(x.a))]:
        return A(a=n)

    inputs_path = tmp_path_factory.mktemp("inputs")
    outputs_path = tmp_path_factory.mktemp("outputs")

    with get_flyte_context(tmp_path_factory, outputs_path) as ctx:
        input_literal_map = _literal_models.LiteralMap(
            {
                "n": _literal_models.Literal(
                    scalar=_literal_models.Scalar(primitive=_literal_models.Primitive(integer=1234)),
                )
            }
        )

        write_proto_to_file(input_literal_map.to_flyte_idl(), str(inputs_path/"inputs.pb"))

        # All literals should be offloaded
        with mock.patch.dict(os.environ, {"_F_L_MIN_SIZE_MB": "0"}):
            _dispatch_execute(ctx, lambda: t1, str(inputs_path/"inputs.pb"), str(outputs_path.absolute()))

            assert "error.pb" not in os.listdir(outputs_path)

            for ff in os.listdir(outputs_path):
                with open(outputs_path/ff, "rb") as f:
                    if ff == "outputs.pb":
                        lit = literals_pb2.LiteralMap()
                        lit.ParseFromString(f.read())
                        assert len(lit.literals) == 1

                        # o0 is offloaded
                        assert "o0" in lit.literals
                        assert lit.literals["o0"].HasField("offloaded_metadata") is True
                        assert lit.literals["o0"].offloaded_metadata.size_bytes > 0
                        assert lit.literals["o0"].offloaded_metadata.uri.endswith("/o0_offloaded_metadata.pb")
                        assert lit.literals["o0"].hash == "1234"
                        assert lit.literals["o0"].offloaded_metadata.inferred_type == t1.interface.outputs["o0"].type.to_flyte_idl()
                    elif ff == "o0_offloaded_metadata.pb":
                        lit = literals_pb2.Literal()
                        lit.ParseFromString(f.read())
                        assert lit.hash == "1234"
                        transformer = TypeEngine.get_transformer(A)
                        a = transformer.to_python_value(ctx, _literal_models.Literal.from_flyte_idl(lit), A)
                        assert a.a == 1234
                    else:
                        assert False, f"Unexpected file {ff}"


def test_dispatch_execute_offloaded_nested_lists_of_literals(tmp_path_factory):
    @task
    def t1(a: typing.List[int]) -> typing.List[typing.List[str]]:
        return [[f"string is: {x}" for x in a] for _ in range(len(a))]

    inputs_path = tmp_path_factory.mktemp("inputs")
    outputs_path = tmp_path_factory.mktemp("outputs")

    ctx = context_manager.FlyteContext.current_context()
    with get_flyte_context(tmp_path_factory, outputs_path) as ctx:
        xs: typing.List[int] = [1, 2, 3]
        input_literal_map = _literal_models.LiteralMap(
            {
                "a": TypeEngine.to_literal(ctx, xs, typing.List[int], TypeEngine.to_literal_type(typing.List[int])),
            }
        )

        write_proto_to_file(input_literal_map.to_flyte_idl(), str(inputs_path/"inputs.pb"))

        with mock.patch.dict(os.environ, {"_F_L_MIN_SIZE_MB": "0"}):
            _dispatch_execute(ctx, lambda: t1, str(inputs_path/"inputs.pb"), str(outputs_path.absolute()))

            assert "error.pb" not in os.listdir(outputs_path)

            for ff in os.listdir(outputs_path):
                with open(outputs_path/ff, "rb") as f:
                    if ff == "outputs.pb":
                        lit = literals_pb2.LiteralMap()
                        lit.ParseFromString(f.read())
                        assert len(lit.literals) == 1
                        assert "o0" in lit.literals
                        assert lit.literals["o0"].HasField("offloaded_metadata") == True
                        assert lit.literals["o0"].offloaded_metadata.size_bytes == 195
                        assert lit.literals["o0"].offloaded_metadata.uri.endswith("/o0_offloaded_metadata.pb")
                        assert lit.literals["o0"].offloaded_metadata.inferred_type == LiteralType(collection_type=LiteralType(collection_type=LiteralType(simple=SimpleType.STRING))).to_flyte_idl()
                    elif ff == "o0_offloaded_metadata.pb":
                        lit = literals_pb2.Literal()
                        lit.ParseFromString(f.read())
                        expected_output = [[f"string is: {x}" for x in xs] for _ in range(len(xs))]
                        assert lit == TypeEngine.to_literal(ctx, expected_output, typing.List[typing.List[str]], TypeEngine.to_literal_type(typing.List[typing.List[str]])).to_flyte_idl()
                    else:
                        assert False, f"Unexpected file {ff}"


def test_dispatch_execute_offloaded_nested_lists_of_literals_offloading_disabled(tmp_path_factory):
    @task
    def t1(a: typing.List[int]) -> typing.List[typing.List[str]]:
        return [[f"string is: {x}" for x in a] for _ in range(len(a))]

    inputs_path = tmp_path_factory.mktemp("inputs")
    outputs_path = tmp_path_factory.mktemp("outputs")

    ctx = context_manager.FlyteContext.current_context()
    with get_flyte_context(tmp_path_factory, outputs_path) as ctx:
        xs: typing.List[int] = [1, 2, 3]
        input_literal_map = _literal_models.LiteralMap(
            {
                "a": TypeEngine.to_literal(ctx, xs, typing.List[int], TypeEngine.to_literal_type(typing.List[int])),
            }
        )

        write_proto_to_file(input_literal_map.to_flyte_idl(), str(inputs_path/"inputs.pb"))

        # Ensure that this is not set by an external source
        assert os.environ.get("_F_L_MIN_SIZE_MB") is None

        # Notice how we're setting the env var to None, which disables offloading completely
        _dispatch_execute(ctx, lambda: t1, str(inputs_path/"inputs.pb"), str(outputs_path.absolute()))

        assert "error.pb" not in os.listdir(outputs_path)

        for ff in os.listdir(outputs_path):
            with open(outputs_path/ff, "rb") as f:
                if ff == "outputs.pb":
                    lit = literals_pb2.LiteralMap()
                    lit.ParseFromString(f.read())
                    assert len(lit.literals) == 1
                    assert "o0" in lit.literals
                    assert lit.literals["o0"].HasField("offloaded_metadata") == False
                else:
                    assert False, f"Unexpected file {ff}"



def test_dispatch_execute_offloaded_map_task(tmp_path_factory):
    @task
    def t1(n: int) -> int:
        return n + 1

    inputs: typing.List[int] = [1, 2, 3, 4]
    for i, v in enumerate(inputs):
        inputs_path = tmp_path_factory.mktemp("inputs")
        outputs_path = tmp_path_factory.mktemp("outputs")

        ctx = context_manager.FlyteContext.current_context()
        with get_flyte_context(tmp_path_factory, outputs_path) as ctx:
            input_literal_map = _literal_models.LiteralMap(
                {
                    "n": TypeEngine.to_literal(ctx, inputs, typing.List[int], TypeEngine.to_literal_type(typing.List[int])),
                }
            )

            write_proto_to_file(input_literal_map.to_flyte_idl(), str(inputs_path/"inputs.pb"))

            with mock.patch.dict(
                    os.environ,
                    {
                        "_F_L_MIN_SIZE_MB": "0", # Always offload
                        "BATCH_JOB_ARRAY_INDEX_OFFSET": str(i),
                    }):
                _dispatch_execute(ctx, lambda: ArrayNodeMapTask(python_function_task=t1), str(inputs_path/"inputs.pb"), str(outputs_path.absolute()), is_map_task=True)

                assert "error.pb" not in os.listdir(outputs_path)

                for ff in os.listdir(outputs_path):
                    with open(outputs_path/ff, "rb") as f:
                        if ff == "outputs.pb":
                            lit = literals_pb2.LiteralMap()
                            lit.ParseFromString(f.read())
                            assert len(lit.literals) == 1
                            assert "o0" in lit.literals
                            assert lit.literals["o0"].HasField("offloaded_metadata") == True
                            assert lit.literals["o0"].offloaded_metadata.uri.endswith("/o0_offloaded_metadata.pb")
                            assert lit.literals["o0"].offloaded_metadata.inferred_type == LiteralType(simple=SimpleType.INTEGER).to_flyte_idl()
                        elif ff == "o0_offloaded_metadata.pb":
                            lit = literals_pb2.Literal()
                            lit.ParseFromString(f.read())
                            expected_output = v + 1
                            assert lit == TypeEngine.to_literal(ctx, expected_output, int, TypeEngine.to_literal_type(int)).to_flyte_idl()
                        else:
                            assert False, f"Unexpected file {ff}"


def test_dispatch_execute_offloaded_nested_lists_of_literals_offloading_disabled(tmp_path_factory):
    @task
    def t1(a: typing.List[int]) -> typing.List[typing.List[str]]:
        return [[f"string is: {x}" for x in a] for _ in range(len(a))]

    inputs_path = tmp_path_factory.mktemp("inputs")
    outputs_path = tmp_path_factory.mktemp("outputs")

    ctx = context_manager.FlyteContext.current_context()
    with get_flyte_context(tmp_path_factory, outputs_path) as ctx:
        xs: typing.List[int] = [1, 2, 3]
        input_literal_map = _literal_models.LiteralMap(
            {
                "a": TypeEngine.to_literal(ctx, xs, typing.List[int], TypeEngine.to_literal_type(typing.List[int])),
            }
        )

        write_proto_to_file(input_literal_map.to_flyte_idl(), str(inputs_path/"inputs.pb"))

        # Ensure that this is not set by an external source
        assert os.environ.get("_F_L_MIN_SIZE_MB") is None

        # Notice how we're setting the env var to None, which disables offloading completely
        _dispatch_execute(ctx, lambda: t1, str(inputs_path/"inputs.pb"), str(outputs_path.absolute()))

        assert "error.pb" not in os.listdir(outputs_path)

        for ff in os.listdir(outputs_path):
            with open(outputs_path/ff, "rb") as f:
                if ff == "outputs.pb":
                    lit = literals_pb2.LiteralMap()
                    lit.ParseFromString(f.read())
                    assert len(lit.literals) == 1
                    assert "o0" in lit.literals
                    assert lit.literals["o0"].HasField("offloaded_metadata") == False
                else:
                    assert False, f"Unexpected file {ff}"
