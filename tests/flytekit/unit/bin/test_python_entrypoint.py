import os

import mock
import six
from click.testing import CliRunner
from flyteidl.core import literals_pb2 as _literals_pb2
from flyteidl.core.errors_pb2 import ErrorDocument

from flytekit.bin.entrypoint import _dispatch_execute, _legacy_execute_task, execute_task_cmd
from flytekit.common import constants as _constants
from flytekit.common import utils as _utils
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import TemporaryConfiguration as _TemporaryConfiguration
from flytekit.core import context_manager
from flytekit.core.base_task import IgnoreOutputs
from flytekit.core.promise import VoidPromise
from flytekit.models import literals as _literal_models
from flytekit.models import literals as _literals
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
            mapped_index = _literals.Literal(_literals.Scalar(primitive=_literals.Primitive(integer=1)))
            index_lookup_collection = _literals.LiteralCollection([mapped_index])
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
@mock.patch("flytekit.interfaces.data.data_proxy.FileAccessProvider.get_data")
@mock.patch("flytekit.interfaces.data.data_proxy.FileAccessProvider.upload_directory")
@mock.patch("flytekit.common.utils.write_proto_to_file")
def test_dispatch_execute_void(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION))) as ctx:
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
@mock.patch("flytekit.interfaces.data.data_proxy.FileAccessProvider.get_data")
@mock.patch("flytekit.interfaces.data.data_proxy.FileAccessProvider.upload_directory")
@mock.patch("flytekit.common.utils.write_proto_to_file")
def test_dispatch_execute_ignore(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True
    ctx = context_manager.FlyteContext.current_context()

    # IgnoreOutputs
    with context_manager.FlyteContextManager.with_context(ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION))) as ctx:
        python_task = mock.MagicMock()
        python_task.dispatch_execute.side_effect = IgnoreOutputs()

        empty_literal_map = _literal_models.LiteralMap({}).to_flyte_idl()
        mock_load_proto.return_value = empty_literal_map

        _dispatch_execute(ctx, python_task, "inputs path", "outputs prefix")
        assert mock_write_to_file.call_count == 0


@mock.patch("flytekit.common.utils.load_proto_from_file")
@mock.patch("flytekit.interfaces.data.data_proxy.FileAccessProvider.get_data")
@mock.patch("flytekit.interfaces.data.data_proxy.FileAccessProvider.upload_directory")
@mock.patch("flytekit.common.utils.write_proto_to_file")
def test_dispatch_execute_exception(mock_write_to_file, mock_upload_dir, mock_get_data, mock_load_proto):
    # Just leave these here, mock them out so nothing happens
    mock_get_data.return_value = True
    mock_upload_dir.return_value = True

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION))) as ctx:
        python_task = mock.MagicMock()
        python_task.dispatch_execute.side_effect = Exception("random")

        empty_literal_map = _literal_models.LiteralMap({}).to_flyte_idl()
        mock_load_proto.return_value = empty_literal_map

        def verify_output(*args, **kwargs):
            assert isinstance(args[0], ErrorDocument)

        mock_write_to_file.side_effect = verify_output
        _dispatch_execute(ctx, python_task, "inputs path", "outputs prefix")
        assert mock_write_to_file.call_count == 1
