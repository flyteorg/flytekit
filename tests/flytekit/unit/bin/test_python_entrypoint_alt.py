from __future__ import absolute_import

import os

import six
from click.testing import CliRunner
from dateutil import parser
from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit.bin.entrypoint_alt import _execute_task, execute_task_cmd, SAGEMAKER_CONTAINER_LOCAL_INPUT_PREFIX
from flytekit.common import constants as _constants
from flytekit.common import utils as _utils
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import TemporaryConfiguration as _TemporaryConfiguration
from flytekit.models import literals as _literal_models
from tests.flytekit.common import task_definitions as _task_defs


def _type_map_from_variable_map(variable_map):
    return {k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in six.iteritems(variable_map)}


def test_single_step_entrypoint_in_proc():
    with _TemporaryConfiguration(
        os.path.join(os.path.dirname(__file__), "fake.config"),
        internal_overrides={"project": "test", "domain": "development"},
    ):
        raw_args = (
            "--train", "/local/host",
            "--validation", "s3://dummy",
            "--a", "1",
            "--b", "0.5",
            "--c", "val",
            "--d", "0",
            "--e", "20180612T09:55:22Z")
        with _utils.AutoDeletingTempDir("out") as output_dir:
            _execute_task(
                task_module=_task_defs.dummy_for_entrypoint_alt.task_module,
                task_name=_task_defs.dummy_for_entrypoint_alt.task_function_name,
                output_prefix=output_dir.name,
                test=False,
                sagemaker_args=raw_args,
            )
            p = _utils.load_proto_from_file(
                _literals_pb2.LiteralMap, os.path.join(output_dir.name, _constants.OUTPUT_FILE_NAME),
            )

            raw_args_map = {}
            for i in range(0, len(raw_args), 2):
                raw_args_map[raw_args[i][2:]] = raw_args[i + 1]

            raw_map = _type_helpers.unpack_literal_map_to_sdk_python_std(
                _literal_models.LiteralMap.from_flyte_idl(p),
                _type_map_from_variable_map(_task_defs.dummy_for_entrypoint_alt.interface.outputs),
            )

            assert len(raw_map) == 7
            assert raw_map["otrain"].uri.rstrip("/") == "{}/{}".format(SAGEMAKER_CONTAINER_LOCAL_INPUT_PREFIX, "train")
            assert raw_map["ovalidation"].uri.rstrip("/") == "{}/{}".format(SAGEMAKER_CONTAINER_LOCAL_INPUT_PREFIX, "validation")
            assert raw_map["oa"] == 1
            assert raw_map["ob"] == 0.5
            assert raw_map["oc"] == "val"
            assert raw_map["od"] is False
            assert raw_map["oe"] == parser.parse("20180612T09:55:22Z")


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

            raw_args = (
                "--train", "s3://dummy",
                "--validation", "s3://dummy",
                "--a", "1",
                "--b", "0.5",
                "--c", "val",
                "--d", "0",
                "--e", "20180612T09:55:22Z")

            with _utils.AutoDeletingTempDir("out") as output_dir:
                cmd = []
                cmd.extend(["--task-module", _task_defs.dummy_for_entrypoint_alt.task_module])
                cmd.extend(["--task-name", _task_defs.dummy_for_entrypoint_alt.task_function_name])
                cmd.extend(["--output-prefix", output_dir.name])
                cmd.extend(raw_args)
                result = CliRunner().invoke(execute_task_cmd, cmd)

                assert result.exit_code == 0
                p = _utils.load_proto_from_file(
                    _literals_pb2.LiteralMap, os.path.join(output_dir.name, _constants.OUTPUT_FILE_NAME),
                )

                raw_args_map = {}
                for i in range(0, len(raw_args), 2):
                    raw_args_map[raw_args[i][2:]] = raw_args[i + 1]

                raw_map = _type_helpers.unpack_literal_map_to_sdk_python_std(
                    _literal_models.LiteralMap.from_flyte_idl(p),
                    _type_map_from_variable_map(_task_defs.dummy_for_entrypoint_alt.interface.outputs),
                )

                assert len(raw_map) == 7
                assert raw_map["otrain"].uri.rstrip("/") == "{}/{}".format(
                    SAGEMAKER_CONTAINER_LOCAL_INPUT_PREFIX, "train")
                assert raw_map["ovalidation"].uri.rstrip("/") == "{}/{}".format(
                    SAGEMAKER_CONTAINER_LOCAL_INPUT_PREFIX, "validation")
                assert raw_map["oa"] == 1
                assert raw_map["ob"] == 0.5
                assert raw_map["oc"] == "val"
                assert raw_map["od"] is False
                assert raw_map["oe"] == parser.parse("20180612T09:55:22Z")
