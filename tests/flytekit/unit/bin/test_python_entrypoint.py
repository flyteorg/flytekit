import os
import typing
from collections import OrderedDict

import mock
import pytest
from flyteidl.core.errors_pb2 import ErrorDocument

from flytekit.bin.entrypoint import _dispatch_execute, normalize_inputs, setup_execution
from flytekit.core import context_manager
from flytekit.core.base_task import IgnoreOutputs
from flytekit.core.data_persistence import DiskPersistence
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.promise import VoidPromise
from flytekit.core.task import task
from flytekit.core.type_engine import TypeEngine
from flytekit.exceptions import user as user_exceptions
from flytekit.exceptions.scopes import system_entry_point
from flytekit.extras.persistence.gcs_gsutil import GCSPersistence
from flytekit.extras.persistence.s3_awscli import S3Persistence
from flytekit.models import literals as _literal_models
from flytekit.models.core import errors as error_models
from flytekit.models.core import execution as execution_models


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
        _dispatch_execute(ctx, python_task, "inputs path", "outputs prefix")
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
        python_task.dispatch_execute.side_effect = IgnoreOutputs()

        empty_literal_map = _literal_models.LiteralMap({}).to_flyte_idl()
        mock_load_proto.return_value = empty_literal_map

        # The system_entry_point decorator does different thing based on whether or not it's the
        # first time it's called. Using it here to mimic the fact that _dispatch_execute is
        # called by _execute_task, which also has a system_entry_point
        system_entry_point(_dispatch_execute)(ctx, python_task, "inputs path", "outputs prefix")
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

        mock_write_to_file.side_effect = verify_output
        _dispatch_execute(ctx, python_task, "inputs path", "outputs prefix")
        assert mock_write_to_file.call_count == 1


@mock.patch.dict(os.environ, {"FLYTE_FAIL_ON_ERROR": "True"})
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

        with pytest.raises(SystemExit) as cm:
            _dispatch_execute(ctx, python_task, "inputs path", "outputs prefix")
            pytest.assertEqual(cm.value.code, 1)


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
        system_entry_point(_dispatch_execute)(ctx, t1, "inputs path", "outputs prefix")
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
        system_entry_point(_dispatch_execute)(ctx, t1, "inputs path", "outputs prefix")
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
        system_entry_point(_dispatch_execute)(ctx, my_subwf, "inputs path", "outputs prefix")
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
        system_entry_point(_dispatch_execute)(ctx, python_task, "inputs path", "outputs prefix")
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
        assert isinstance(ctx.file_access._default_remote, DiskPersistence)


def test_setup_cloud_prefix():
    with setup_execution("s3://", checkpoint_path=None, prev_checkpoint=None) as ctx:
        assert isinstance(ctx.file_access._default_remote, S3Persistence)

    with setup_execution("gs://", checkpoint_path=None, prev_checkpoint=None) as ctx:
        assert isinstance(ctx.file_access._default_remote, GCSPersistence)


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
