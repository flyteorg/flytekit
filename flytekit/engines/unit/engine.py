import logging as _logging
import os as _os
from datetime import datetime as _datetime

import six as _six
from flyteidl.plugins import qubole_pb2 as _qubole_pb2
from google.protobuf.json_format import ParseDict as _ParseDict
from six import moves as _six_moves

from flytekit.common import constants as _sdk_constants
from flytekit.common import utils as _common_utils
from flytekit.common.exceptions import system as _system_exception
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import TemporaryConfiguration as _TemporaryConfiguration
from flytekit.engines import common as _common_engine
from flytekit.engines.unit.mock_stats import MockStats
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import array_job as _array_job
from flytekit.models import literals as _literals
from flytekit.models import qubole as _qubole_models
from flytekit.models.core.identifier import WorkflowExecutionIdentifier


class UnitTestEngineFactory(_common_engine.BaseExecutionEngineFactory):
    def get_task(self, sdk_task):
        """
        :param flytekit.common.tasks.task.SdkTask sdk_task:
        :rtype: UnitTestEngineTask
        """
        if sdk_task.type in {
            _sdk_constants.SdkTaskType.PYTHON_TASK,
            _sdk_constants.SdkTaskType.SPARK_TASK,
            _sdk_constants.SdkTaskType.SENSOR_TASK,
        }:
            return ReturnOutputsTask(sdk_task)
        elif sdk_task.type in {
            _sdk_constants.SdkTaskType.DYNAMIC_TASK,
        }:
            return DynamicTask(sdk_task)
        elif sdk_task.type in {
            _sdk_constants.SdkTaskType.BATCH_HIVE_TASK,
        }:
            return HiveTask(sdk_task)
        else:
            raise _user_exceptions.FlyteAssertion(
                "Unit tests are not currently supported for tasks of type: {}".format(sdk_task.type)
            )

    def get_workflow(self, _):
        raise _user_exceptions.FlyteAssertion("Unit testing of workflows is not currently supported")

    def get_launch_plan(self, _):
        raise _user_exceptions.FlyteAssertion("Unit testing of launch plans is not currently supported")

    def get_task_execution(self, _):
        raise _user_exceptions.FlyteAssertion("Unit testing does not return execution handles.")

    def get_node_execution(self, _):
        raise _user_exceptions.FlyteAssertion("Unit testing does not return execution handles.")

    def get_workflow_execution(self, _):
        raise _user_exceptions.FlyteAssertion("Unit testing does not return execution handles.")

    def fetch_workflow_execution(self, _):
        raise _user_exceptions.FlyteAssertion("Unit testing does not fetch execution handles.")

    def fetch_task(self, _):
        raise _user_exceptions.FlyteAssertion("Unit testing does not fetch real tasks.")

    def fetch_latest_task(self, named_task):
        raise _user_exceptions.FlyteAssertion("Unit testing does not fetch the real latest task.")

    def fetch_launch_plan(self, _):
        raise _user_exceptions.FlyteAssertion("Unit testing does not fetch real launch plans.")

    def fetch_workflow(self, _):
        raise _user_exceptions.FlyteAssertion("Unit testing does not fetch real workflows.")


class UnitTestEngineTask(_common_engine.BaseTaskExecutor):
    def execute(self, inputs, context=None):
        """
        Just execute the function and return the outputs as a user-readable dictionary.
        :param flytekit.models.literals.LiteralMap inputs:
        :param context:
        :rtype: dict[Text,flytekit.models.common.FlyteIdlEntity]
        """
        with _TemporaryConfiguration(
            _os.path.join(_os.path.dirname(__file__), "unit.config"), internal_overrides={"image": "unit_image"},
        ):
            with _common_utils.AutoDeletingTempDir("unit_test_dir") as working_directory:
                with _data_proxy.LocalWorkingDirectoryContext(working_directory):
                    return self._transform_for_user_output(self._execute_user_code(inputs))

    def _execute_user_code(self, inputs):
        """
        :param flytekit.models.literals.LiteralMap inputs:
        :rtype: dict[Text,flytekit.models.common.FlyteIdlEntity]
        """
        with _common_utils.AutoDeletingTempDir("user_dir") as user_working_directory:
            return self.sdk_task.execute(
                _common_engine.EngineContext(
                    execution_id=WorkflowExecutionIdentifier(project="unit_test", domain="unit_test", name="unit_test"),
                    execution_date=_datetime.utcnow(),
                    stats=MockStats(),
                    logging=_logging,  # TODO: A mock logging object that we can read later.
                    tmp_dir=user_working_directory,
                ),
                inputs,
            )

    def _transform_for_user_output(self, outputs):
        """
        Take whatever is returned from the task execution and convert to a reasonable output for the behavior of this
        task's unit test.
        :param dict[Text,flytekit.models.common.FlyteIdlEntity] outputs:
        :rtype: T
        """
        return outputs

    def register(self, identifier, version):
        raise _user_exceptions.FlyteAssertion("You cannot register unit test tasks.")

    def launch(
        self,
        project,
        domain,
        name=None,
        inputs=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
        auth_role=None,
    ):
        raise _user_exceptions.FlyteAssertion("You cannot launch unit test tasks.")


class ReturnOutputsTask(UnitTestEngineTask):
    def _transform_for_user_output(self, outputs):
        """
        Just return the outputs as a user-readable dictionary.
        :param dict[Text,flytekit.models.common.FlyteIdlEntity] outputs:
        :rtype: T
        """
        literal_map = outputs[_sdk_constants.OUTPUT_FILE_NAME]
        return {
            name: _type_helpers.get_sdk_value_from_literal(
                literal_map.literals[name], sdk_type=_type_helpers.get_sdk_type_from_literal_type(variable.type),
            ).to_python_std()
            for name, variable in _six.iteritems(self.sdk_task.interface.outputs)
        }


class DynamicTask(ReturnOutputsTask):
    def __init__(self, *args, **kwargs):
        self._has_workflow_node = False
        super(DynamicTask, self).__init__(*args, **kwargs)

    def _transform_for_user_output(self, outputs):
        if self.has_workflow_node:
            # If a workflow node has been detected, then we skip any transformation
            # This is to support the early termination behavior of the unit test engine when it comes to dynamic tasks
            # that produce launch plan or subworkflow nodes.
            # See the warning message in the code below for additional information
            return outputs
        return super(DynamicTask, self)._transform_for_user_output(outputs)

    def _execute_user_code(self, inputs):
        """
        :param flytekit.models.literals.LiteralMap inputs:
        :rtype: dict[Text,flytekit.models.common.FlyteIdlEntity]
        """
        results = super(DynamicTask, self)._execute_user_code(inputs)
        if _sdk_constants.FUTURES_FILE_NAME in results:
            futures = results[_sdk_constants.FUTURES_FILE_NAME]
            sub_task_outputs = {}
            tasks_map = {task.id: task for task in futures.tasks}

            for future_node in futures.nodes:
                if future_node.workflow_node is not None:
                    # TODO: implement proper unit testing for launchplan and subworkflow nodes somehow
                    _logging.warning(
                        "A workflow node has been detected in the output of the dynamic task. The "
                        "Flytekit unit test engine is incomplete for dynamic tasks that return launch "
                        "plans or subworkflows. The generated dynamic job spec will be returned but "
                        "they will not be run."
                    )
                    # For now, just return the output of the parent task
                    self._has_workflow_node = True
                    return results
                task = tasks_map[future_node.task_node.reference_id]
                if task.type == _sdk_constants.SdkTaskType.CONTAINER_ARRAY_TASK:
                    sub_task_output = DynamicTask.execute_array_task(future_node.id, task, results)
                elif task.type == _sdk_constants.SdkTaskType.SPARK_TASK:
                    # This is required because `_transform_for_user_output` function about is invoked which
                    # checks for outputs
                    self._has_workflow_node = True
                    return results
                elif task.type == _sdk_constants.SdkTaskType.HIVE_JOB:
                    # TODO: futures.outputs should have the Schema instances.
                    # After schema is implemented, fill out random data into the random locations
                    # then check output in test function
                    # Even though we recommend people use typed schemas, they might not always do so...
                    # in which case it'll be impossible to predict the actual schema, we should support a
                    # way for unit test authors to provide fake data regardless
                    sub_task_output = None
                else:
                    inputs_path = _os.path.join(future_node.id, _sdk_constants.INPUT_FILE_NAME)
                    if inputs_path not in results:
                        raise _system_exception.FlyteSystemAssertion(
                            "dynamic task hasn't generated expected inputs document [{}] found {}".format(
                                future_node.id, list(results.keys())
                            )
                        )
                    sub_task_output = UnitTestEngineFactory().get_task(task).execute(results[inputs_path])
                sub_task_outputs[future_node.id] = sub_task_output

            results[_sdk_constants.OUTPUT_FILE_NAME] = _literals.LiteralMap(
                literals={
                    binding.var: DynamicTask.fulfil_bindings(binding.binding, sub_task_outputs)
                    for binding in futures.outputs
                }
            )
        return results

    @property
    def has_workflow_node(self):
        """
        :rtype: bool
        """
        return self._has_workflow_node

    @staticmethod
    def execute_array_task(root_input_path, task, array_inputs):
        array_job = _array_job.ArrayJob.from_dict(task.custom)
        outputs = {}
        for job_index in _six_moves.range(0, array_job.size):
            inputs_path = _os.path.join(root_input_path, _six.text_type(job_index), _sdk_constants.INPUT_FILE_NAME,)
            if inputs_path not in array_inputs:
                raise _system_exception.FlyteSystemAssertion(
                    "dynamic task hasn't generated expected inputs document [{}].".format(inputs_path)
                )

            input_proto = array_inputs[inputs_path]
            # All outputs generated by the same array job will have the same key in sub_task_outputs,
            # they will, however, differ in the var names; they will be on the format [<job_index>].<var_name>
            # e.g. [1].out1
            for key, val in _six.iteritems(
                ReturnOutputsTask(
                    task.assign_type_and_return(_sdk_constants.SdkTaskType.PYTHON_TASK)  # TODO: This is weird
                ).execute(input_proto)
            ):
                outputs["[{}].{}".format(job_index, key)] = val
        return outputs

    @staticmethod
    def fulfil_bindings(binding_data, fulfilled_promises):
        """
        Substitutes promise values in binding_data with model Literal values built from python std values in
        fulfilled_promises

        :param _interface.BindingData binding_data:
        :param dict[Text,T] fulfilled_promises:
        :rtype:
        """
        if binding_data.scalar:
            return _literals.Literal(scalar=binding_data.scalar)
        elif binding_data.collection:
            return _literals.Literal(
                collection=_literals.LiteralCollection(
                    [
                        DynamicTask.fulfil_bindings(sub_binding_data, fulfilled_promises)
                        for sub_binding_data in binding_data.collection.bindings
                    ]
                )
            )
        elif binding_data.promise:
            if binding_data.promise.node_id not in fulfilled_promises:
                raise _system_exception.FlyteSystemAssertion(
                    "Expecting output of node [{}] but that hasn't been produced.".format(binding_data.promise.node_id)
                )
            node_output = fulfilled_promises[binding_data.promise.node_id]
            if binding_data.promise.var not in node_output:
                raise _system_exception.FlyteSystemAssertion(
                    "Expecting output [{}] of node [{}] but that hasn't been produced.".format(
                        binding_data.promise.var, binding_data.promise.node_id
                    )
                )

            return binding_data.promise.sdk_type.from_python_std(node_output[binding_data.promise.var])
        elif binding_data.map:
            return _literals.Literal(
                map=_literals.LiteralMap(
                    {
                        k: DynamicTask.fulfil_bindings(sub_binding_data, fulfilled_promises)
                        for k, sub_binding_data in _six.iteritems(binding_data.map.bindings)
                    }
                )
            )


class HiveTask(DynamicTask):
    def _transform_for_user_output(self, outputs):
        """
        Just execute the function and return the list of Hive queries returned.
        :param dict[Text,flytekit.models.common.FlyteIdlEntity] outputs:
        :rtype: list[Text]
        """
        futures = outputs.get(_sdk_constants.FUTURES_FILE_NAME)
        if futures:
            queries = []
            task_ids_to_defs = {
                t.id.name: _qubole_models.QuboleHiveJob.from_flyte_idl(
                    _ParseDict(t.custom, _qubole_pb2.QuboleHiveJob())
                )
                for t in futures.tasks
            }
            for node in futures.nodes:
                queries.append(task_ids_to_defs[node.task_node.reference_id.name].query.query)
            return queries
        else:
            return []
