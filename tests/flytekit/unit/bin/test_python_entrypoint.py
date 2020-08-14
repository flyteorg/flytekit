from __future__ import absolute_import

import os

import six
from click.testing import CliRunner
from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit.bin.entrypoint import _execute_task, execute_task_cmd
from flytekit.common import constants as _constants
from flytekit.common import utils as _utils
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import TemporaryConfiguration as _TemporaryConfiguration
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
                {"a": 9}, _type_map_from_variable_map(_task_defs.add_one.interface.inputs),
            )
            input_file = os.path.join(input_dir.name, "inputs.pb")
            _utils.write_proto_to_file(literal_map.to_flyte_idl(), input_file)

            with _utils.AutoDeletingTempDir("out") as output_dir:
                _execute_task(
                    _task_defs.add_one.task_module,
                    _task_defs.add_one.task_function_name,
                    input_file,
                    output_dir.name,
                    False,
                )

                p = _utils.load_proto_from_file(
                    _literals_pb2.LiteralMap, os.path.join(output_dir.name, _constants.OUTPUT_FILE_NAME),
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
                {"a": 9}, _type_map_from_variable_map(_task_defs.add_one.interface.inputs),
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
                    _literals_pb2.LiteralMap, os.path.join(output_dir.name, _constants.OUTPUT_FILE_NAME),
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
                {"a": 9}, _type_map_from_variable_map(_task_defs.add_one.interface.inputs),
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

            _execute_task(
                _task_defs.add_one.task_module, _task_defs.add_one.task_function_name, dir.name, dir.name, False,
            )

            raw_map = _type_helpers.unpack_literal_map_to_sdk_python_std(
                _literal_models.LiteralMap.from_flyte_idl(
                    _utils.load_proto_from_file(
                        _literals_pb2.LiteralMap, os.path.join(input_dir, _constants.OUTPUT_FILE_NAME),
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
