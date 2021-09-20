import os
import typing
from collections import OrderedDict

import mock
import pytest
import six
from click.testing import CliRunner
from flyteidl.core import literals_pb2 as _literals_pb2
from flyteidl.core.errors_pb2 import ErrorDocument

from flytekit.bin.entrypoint import _dispatch_execute, _legacy_execute_task, execute_task_cmd, setup_execution
from flytekit.common import constants as _constants
from flytekit.common import utils as _utils
from flytekit.common.exceptions import user as user_exceptions
from flytekit.common.exceptions.scopes import system_entry_point
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import TemporaryConfiguration as _TemporaryConfiguration
from flytekit.core import context_manager
from flytekit.core.base_task import IgnoreOutputs
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.promise import VoidPromise
from flytekit.core.task import task
from flytekit.core.type_engine import TypeEngine
from flytekit.extras.persistence.gcs_gsutil import GCSPersistence
from flytekit.extras.persistence.s3_awscli import S3Persistence
from flytekit.models import literals as _literal_models
from flytekit.models.core import errors as error_models
from flytekit.models.core import execution as execution_models
from tests.flytekit.common import task_definitions as _task_defs


def _type_map_from_variable_map(variable_map):
    return {k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in six.iteritems(variable_map)}


def test_single_step_entrypoint_in_proc():
    with _TemporaryConfiguration(
        os.path.join(os.path.dirname(__file__), "fake.config"),
        internal_overrides={"project": "test", "domain": "development"},
    ):
        with _utils.AutoDeletingTempDir("in") as input_dir:
            literal_map = _type_helpers.pack_python_std_map_to_literal_map(
                {"a": 9},
                _type_map_from_variable_map(_task_defs.add_one.interface.inputs),
            )
            input_file = os.path.join(input_dir.name, "inputs.pb")
            _utils.write_proto_to_file(literal_map.to_flyte_idl(), input_file)

            with _utils.AutoDeletingTempDir("out") as output_dir:
                _legacy_execute_task(
                    _task_defs.add_one.task_module,
                    _task_defs.add_one.task_function_name,
                    input_file,
                    output_dir.name,
                    output_dir.name,
                    False,
                )

                p = _utils.load_proto_from_file(
                    _literals_pb2.LiteralMap,
                    os.path.join(output_dir.name, _constants.OUTPUT_FILE_NAME),
                )
                raw_map = _type_helpers.unpack_literal_map_to_sdk_python_std(
                    _literal_models.LiteralMap.from_flyte_idl(p),
                    _type_map_from_variable_map(_task_defs.add_one.interface.outputs),
                )
                assert raw_map["b"] == 10
                assert len(raw_map) == 1


def test_single_step_entrypoint_out_of_proc():
    with _TemporaryConfiguration(
        os.path.join(os.path.dirname(__file__), "fake.config"),
        internal_overrides={"project": "test", "domain": "development"},
    ):
        with _utils.AutoDeletingTempDir("in") as input_dir:
            literal_map = _type_helpers.pack_python_std_map_to_literal_map(
                {"a": 9},
                _type_map_from_variable_map(_task_defs.add_one.interface.inputs),
            )
            input_file = os.path.join(input_dir.name, "inputs.pb")
            _utils.write_proto_to_file(literal_map.to_flyte_idl(), input_file)

            with _utils.AutoDeletingTempDir("out") as output_dir:
                cmd = []
                cmd.extend(["--task-module", _task_defs.add_one.task_module])
                cmd.extend(["--task-name", _task_defs.add_one.task_function_name])
                cmd.extend(["--inputs", input_file])
                cmd.extend(["--output-prefix", output_dir.name])
                result = CliRunner().invoke(execute_task_cmd, cmd)

                assert result.exit_code == 0
                p = _utils.load_proto_from_file(
                    _literals_pb2.LiteralMap,
                    os.path.join(output_dir.name, _constants.OUTPUT_FILE_NAME),
                )
                raw_map = _type_helpers.unpack_literal_map_to_sdk_python_std(
                    _literal_models.LiteralMap.from_flyte_idl(p),
                    _type_map_from_variable_map(_task_defs.add_one.interface.outputs),
                )
                assert raw_map["b"] == 10
                assert len(raw_map) == 1


def test_arrayjob_entrypoint_in_proc():
    with _TemporaryConfiguration(
        os.path.join(os.path.dirname(__file__), "fake.config"),
        internal_overrides={"project": "test", "domain": "development"},
    ):
        with _utils.AutoDeletingTempDir("dir") as dir:
            literal_map = _type_helpers.pack_python_std_map_to_literal_map(
                {"a": 9},
                _type_map_from_variable_map(_task_defs.add_one.interface.inputs),
            )

            input_dir = os.path.join(dir.name, "1")
            os.mkdir(input_dir)  # auto cleanup will take this subdir into account

            input_file = os.path.join(input_dir, "inputs.pb")
            _utils.write_proto_to_file(literal_map.to_flyte_idl(), input_file)

            # construct indexlookup.pb which has array: [1]
            mapped_index = _literal_models.Literal(
                _literal_models.Scalar(primitive=_literal_models.Primitive(integer=1))
            )
            index_lookup_collection = _literal_models.LiteralCollection([mapped_index])
            index_lookup_file = os.path.join(dir.name, "indexlookup.pb")
            _utils.write_proto_to_file(index_lookup_collection.to_flyte_idl(), index_lookup_file)

            # fake arrayjob task by setting environment variables
            orig_env_index_var_name = os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME")
            orig_env_array_index = os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")
            os.environ["BATCH_JOB_ARRAY_INDEX_VAR_NAME"] = "AWS_BATCH_JOB_ARRAY_INDEX"
            os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "0"

            _legacy_execute_task(
                _task_defs.add_one.task_module,
                _task_defs.add_one.task_function_name,
                dir.name,
                dir.name,
                dir.name,
                False,
            )

            raw_map = _type_helpers.unpack_literal_map_to_sdk_python_std(
                _literal_models.LiteralMap.from_flyte_idl(
                    _utils.load_proto_from_file(
                        _literals_pb2.LiteralMap,
                        os.path.join(input_dir, _constants.OUTPUT_FILE_NAME),
                    )
                ),
                _type_map_from_variable_map(_task_defs.add_one.interface.outputs),
            )
            assert raw_map["b"] == 10
            assert len(raw_map) == 1

            # reset the env vars
            if orig_env_index_var_name:
                os.environ["BATCH_JOB_ARRAY_INDEX_VAR_NAME"] = orig_env_index_var_name
            if orig_env_array_index:
                os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = orig_env_array_index


@mock.patch("flytekit.bin.entrypoint._legacy_execute_task")
def test_backwards_compatible_replacement(mock_legacy_execute_task):
    def return_args(*args, **kwargs):
        assert args[4] is None

    mock_legacy_execute_task.side_effect = return_args

    with _TemporaryConfiguration(
        os.path.join(os.path.dirname(__file__), "fake.config"),
        internal_overrides={"project": "test", "domain": "development"},
    ):
        with _utils.AutoDeletingTempDir("in"):
            with _utils.AutoDeletingTempDir("out"):
                cmd = []
                cmd.extend(["--task-module", "fake"])
                cmd.extend(["--task-name", "fake"])
                cmd.extend(["--inputs", "fake"])
                cmd.extend(["--output-prefix", "fake"])
                cmd.extend(["--raw-output-data-prefix", "{{.rawOutputDataPrefix}}"])
                result = CliRunner().invoke(execute_task_cmd, cmd)
                assert result.exit_code == 0


@mock.patch("flytekit.common.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.common.utils.write_proto_to_file")
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


@mock.patch("flytekit.common.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.common.utils.write_proto_to_file")
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


@mock.patch("flytekit.common.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.common.utils.write_proto_to_file")
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


# This function collects outputs instead of writing them to a file.
# See flytekit.common.utils.write_proto_to_file for the original
def get_output_collector(results: OrderedDict):
    def output_collector(proto, path):
        results[path] = proto

    return output_collector


@mock.patch("flytekit.common.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.common.utils.write_proto_to_file")
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
        input_literal_map = TypeEngine.dict_to_literal_map(ctx, {"a": 5}, {"a": int})
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


@mock.patch("flytekit.common.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.common.utils.write_proto_to_file")
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
        input_literal_map = TypeEngine.dict_to_literal_map(ctx, {"a": 5}, {"a": int})
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


@mock.patch("flytekit.common.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.common.utils.write_proto_to_file")
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
        input_literal_map = TypeEngine.dict_to_literal_map(ctx, {"a": 5}, {"a", int})
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


@mock.patch("flytekit.common.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.common.utils.write_proto_to_file")
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


def test_setup_bad_prefix():
    with pytest.raises(TypeError):
        with setup_execution("qwerty"):
            ...


def test_setup_cloud_prefix():
    with setup_execution("s3://") as ctx:
        assert isinstance(ctx.file_access._default_remote, S3Persistence)

    with setup_execution("gs://") as ctx:
        assert isinstance(ctx.file_access._default_remote, GCSPersistence)
