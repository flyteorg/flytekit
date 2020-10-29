import datetime as _datetime
import os as _os
import unittest
from unittest import mock

import retry.api
from flyteidl.plugins.sagemaker.training_job_pb2 import TrainingJobResourceConfig as _pb2_TrainingJobResourceConfig
from google.protobuf.json_format import ParseDict

from flytekit.common import constants as _common_constants
from flytekit.common import utils as _utils
from flytekit.common.core.identifier import WorkflowExecutionIdentifier
from flytekit.common.tasks import task as _sdk_task
from flytekit.common.tasks.sagemaker import distributed_training as _sm_distribution
from flytekit.common.tasks.sagemaker import hpo_job_task
from flytekit.common.tasks.sagemaker.built_in_training_job_task import SdkBuiltinAlgorithmTrainingJobTask
from flytekit.common.tasks.sagemaker.custom_training_job_task import CustomTrainingJobTask
from flytekit.common.tasks.sagemaker.hpo_job_task import (
    HyperparameterTuningJobConfig,
    SdkSimpleHyperparameterTuningJobTask,
)
from flytekit.common.types import helpers as _type_helpers
from flytekit.engines import common as _common_engine
from flytekit.engines.unit.mock_stats import MockStats
from flytekit.models import literals as _literals
from flytekit.models import types as _idl_types
from flytekit.models.core import identifier as _identifier
from flytekit.models.core import types as _core_types
from flytekit.models.sagemaker.hpo_job import HyperparameterTuningJobConfig as _HyperparameterTuningJobConfig
from flytekit.models.sagemaker.hpo_job import (
    HyperparameterTuningObjective,
    HyperparameterTuningObjectiveType,
    HyperparameterTuningStrategy,
    TrainingJobEarlyStoppingType,
)
from flytekit.models.sagemaker.parameter_ranges import (
    ContinuousParameterRange,
    HyperparameterScalingType,
    IntegerParameterRange,
    ParameterRangeOneOf,
)
from flytekit.models.sagemaker.training_job import (
    AlgorithmName,
    AlgorithmSpecification,
    InputContentType,
    InputMode,
    MetricDefinition,
    TrainingJobResourceConfig,
)
from flytekit.sdk import types as _sdk_types
from flytekit.sdk.sagemaker.task import custom_training_job_task
from flytekit.common.tasks.sagemaker.hpo_job_task import SdkSimpleHyperparameterTuningJobTask
from flytekit.sdk.tasks import inputs, outputs
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import Input, workflow_class

example_hyperparams = {
    "base_score": "0.5",
    "booster": "gbtree",
    "csv_weights": "0",
    "dsplit": "row",
    "grow_policy": "depthwise",
    "lambda_bias": "0.0",
    "max_bin": "256",
    "max_leaves": "0",
    "normalize_type": "tree",
    "objective": "reg:linear",
    "one_drop": "0",
    "prob_buffer_row": "1.0",
    "process_type": "default",
    "rate_drop": "0.0",
    "refresh_leaf": "1",
    "sample_type": "uniform",
    "scale_pos_weight": "1.0",
    "silent": "0",
    "sketch_eps": "0.03",
    "skip_drop": "0.0",
    "tree_method": "auto",
    "tweedie_variance_power": "1.5",
    "updater": "grow_colmaker,prune",
}


def test_builtin_algorithm_training_job_task():
    builtin_algorithm_training_job_task = SdkBuiltinAlgorithmTrainingJobTask(
        training_job_resource_config=TrainingJobResourceConfig(
            instance_type="ml.m4.xlarge", instance_count=1, volume_size_in_gb=25,
        ),
        algorithm_specification=AlgorithmSpecification(
            input_mode=InputMode.FILE,
            input_content_type=InputContentType.TEXT_CSV,
            algorithm_name=AlgorithmName.XGBOOST,
            algorithm_version="0.72",
        ),
    )

    builtin_algorithm_training_job_task._id = _identifier.Identifier(
        _identifier.ResourceType.TASK, "my_project", "my_domain", "my_name", "my_version"
    )
    assert isinstance(builtin_algorithm_training_job_task, SdkBuiltinAlgorithmTrainingJobTask)
    assert isinstance(builtin_algorithm_training_job_task, _sdk_task.SdkTask)
    assert builtin_algorithm_training_job_task.interface.inputs["train"].description == ""
    assert builtin_algorithm_training_job_task.interface.inputs["train"].type == _idl_types.LiteralType(
        blob=_core_types.BlobType(format="csv", dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,)
    )
    assert (
        builtin_algorithm_training_job_task.interface.inputs["train"].type
        == _sdk_types.Types.MultiPartCSV.to_flyte_literal_type()
    )
    assert builtin_algorithm_training_job_task.interface.inputs["validation"].description == ""
    assert (
        builtin_algorithm_training_job_task.interface.inputs["validation"].type
        == _sdk_types.Types.MultiPartCSV.to_flyte_literal_type()
    )
    assert builtin_algorithm_training_job_task.interface.inputs["train"].type == _idl_types.LiteralType(
        blob=_core_types.BlobType(format="csv", dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,)
    )
    assert builtin_algorithm_training_job_task.interface.inputs["static_hyperparameters"].description == ""
    assert (
        builtin_algorithm_training_job_task.interface.inputs["static_hyperparameters"].type
        == _sdk_types.Types.Generic.to_flyte_literal_type()
    )
    assert builtin_algorithm_training_job_task.interface.outputs["model"].description == ""
    assert (
        builtin_algorithm_training_job_task.interface.outputs["model"].type
        == _sdk_types.Types.Blob.to_flyte_literal_type()
    )
    assert builtin_algorithm_training_job_task.type == _common_constants.SdkTaskType.SAGEMAKER_TRAINING_JOB_TASK
    assert builtin_algorithm_training_job_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert builtin_algorithm_training_job_task.metadata.deprecated_error_message == ""
    assert builtin_algorithm_training_job_task.metadata.discoverable is False
    assert builtin_algorithm_training_job_task.metadata.discovery_version == ""
    assert builtin_algorithm_training_job_task.metadata.retries.retries == 0
    assert "metricDefinitions" not in builtin_algorithm_training_job_task.custom["algorithmSpecification"].keys()

    ParseDict(
        builtin_algorithm_training_job_task.custom["trainingJobResourceConfig"], _pb2_TrainingJobResourceConfig(),
    )  # fails the test if it cannot be parsed


builtin_algorithm_training_job_task2 = SdkBuiltinAlgorithmTrainingJobTask(
    training_job_resource_config=TrainingJobResourceConfig(
        instance_type="ml.m4.xlarge", instance_count=1, volume_size_in_gb=25,
    ),
    algorithm_specification=AlgorithmSpecification(
        input_mode=InputMode.FILE,
        input_content_type=InputContentType.TEXT_CSV,
        algorithm_name=AlgorithmName.XGBOOST,
        algorithm_version="0.72",
        metric_definitions=[MetricDefinition(name="Validation error", regex="validation:error")],
    ),
)

simple_xgboost_hpo_job_task = hpo_job_task.SdkSimpleHyperparameterTuningJobTask(
    training_job=builtin_algorithm_training_job_task2,
    max_number_of_training_jobs=10,
    max_parallel_training_jobs=5,
    cache_version="1",
    retries=2,
    cacheable=True,
    tunable_parameters=["num_round", "gamma", "max_depth"],
)

simple_xgboost_hpo_job_task._id = _identifier.Identifier(
    _identifier.ResourceType.TASK, "my_project", "my_domain", "my_name", "my_version"
)


def test_simple_hpo_job_task():
    assert isinstance(simple_xgboost_hpo_job_task, SdkSimpleHyperparameterTuningJobTask)
    assert isinstance(simple_xgboost_hpo_job_task, _sdk_task.SdkTask)
    # Checking if the input of the underlying SdkTrainingJobTask has been embedded
    assert simple_xgboost_hpo_job_task.interface.inputs["train"].description == ""
    assert (
        simple_xgboost_hpo_job_task.interface.inputs["train"].type
        == _sdk_types.Types.MultiPartCSV.to_flyte_literal_type()
    )
    assert simple_xgboost_hpo_job_task.interface.inputs["train"].type == _idl_types.LiteralType(
        blob=_core_types.BlobType(format="csv", dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,)
    )
    assert simple_xgboost_hpo_job_task.interface.inputs["validation"].description == ""
    assert (
        simple_xgboost_hpo_job_task.interface.inputs["validation"].type
        == _sdk_types.Types.MultiPartCSV.to_flyte_literal_type()
    )
    assert simple_xgboost_hpo_job_task.interface.inputs["validation"].type == _idl_types.LiteralType(
        blob=_core_types.BlobType(format="csv", dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,)
    )
    assert simple_xgboost_hpo_job_task.interface.inputs["static_hyperparameters"].description == ""
    assert (
        simple_xgboost_hpo_job_task.interface.inputs["static_hyperparameters"].type
        == _sdk_types.Types.Generic.to_flyte_literal_type()
    )

    # Checking if the hpo-specific input is defined
    assert simple_xgboost_hpo_job_task.interface.inputs["hyperparameter_tuning_job_config"].description == ""
    assert (
        simple_xgboost_hpo_job_task.interface.inputs["hyperparameter_tuning_job_config"].type
        == HyperparameterTuningJobConfig.to_flyte_literal_type()
    )
    assert simple_xgboost_hpo_job_task.interface.outputs["model"].description == ""
    assert simple_xgboost_hpo_job_task.interface.outputs["model"].type == _sdk_types.Types.Blob.to_flyte_literal_type()
    assert simple_xgboost_hpo_job_task.type == _common_constants.SdkTaskType.SAGEMAKER_HYPERPARAMETER_TUNING_JOB_TASK

    # Checking if the spec of the TrainingJob is embedded into the custom field
    # of this SdkSimpleHyperparameterTuningJobTask
    assert simple_xgboost_hpo_job_task.to_flyte_idl().custom["trainingJob"] == (
        builtin_algorithm_training_job_task2.to_flyte_idl().custom
    )

    assert simple_xgboost_hpo_job_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert simple_xgboost_hpo_job_task.metadata.discoverable is True
    assert simple_xgboost_hpo_job_task.metadata.discovery_version == "1"
    assert simple_xgboost_hpo_job_task.metadata.retries.retries == 2

    assert simple_xgboost_hpo_job_task.metadata.deprecated_error_message == ""
    assert "metricDefinitions" in simple_xgboost_hpo_job_task.custom["trainingJob"]["algorithmSpecification"].keys()
    assert len(simple_xgboost_hpo_job_task.custom["trainingJob"]["algorithmSpecification"]["metricDefinitions"]) == 1
    """
    These are attributes for SdkRunnable. We will need these when supporting CustomTrainingJobTask and CustomHPOJobTask
    assert simple_xgboost_hpo_job_task.task_module == __name__
    assert simple_xgboost_hpo_job_task._get_container_definition().args[0] == 'pyflyte-execute'
    """


def test_custom_training_job():
    @inputs(input_1=Types.Integer)
    @outputs(model=Types.Blob)
    @custom_training_job_task(
        training_job_resource_config=TrainingJobResourceConfig(
            instance_type="ml.m4.xlarge", instance_count=1, volume_size_in_gb=25,
        ),
        algorithm_specification=AlgorithmSpecification(
            input_mode=InputMode.FILE,
            input_content_type=InputContentType.TEXT_CSV,
            metric_definitions=[MetricDefinition(name="Validation error", regex="validation:error")],
        ),
    )
    def my_task(wf_params, input_1, model):
        pass

    assert type(my_task) == CustomTrainingJobTask


def test_simple_hpo_job_task_interface():
    @workflow_class
    class MyWf(object):
        train_dataset = Input(Types.Blob)
        validation_dataset = Input(Types.Blob)
        static_hyperparameters = Input(Types.Generic)
        hyperparameter_tuning_job_config = Input(
            HyperparameterTuningJobConfig,
            default=_HyperparameterTuningJobConfig(
                tuning_strategy=HyperparameterTuningStrategy.BAYESIAN,
                tuning_objective=HyperparameterTuningObjective(
                    objective_type=HyperparameterTuningObjectiveType.MINIMIZE, metric_name="validation:error",
                ),
                training_job_early_stopping_type=TrainingJobEarlyStoppingType.AUTO,
            ),
        )

        a = simple_xgboost_hpo_job_task(
            train=train_dataset,
            validation=validation_dataset,
            static_hyperparameters=static_hyperparameters,
            hyperparameter_tuning_job_config=hyperparameter_tuning_job_config,
            num_round=ParameterRangeOneOf(
                IntegerParameterRange(min_value=3, max_value=10, scaling_type=HyperparameterScalingType.LINEAR)
            ),
            max_depth=ParameterRangeOneOf(
                IntegerParameterRange(min_value=5, max_value=7, scaling_type=HyperparameterScalingType.LINEAR)
            ),
            gamma=ParameterRangeOneOf(
                ContinuousParameterRange(min_value=0.0, max_value=0.3, scaling_type=HyperparameterScalingType.LINEAR)
            ),
        )

    assert MyWf.nodes[0].inputs[2].binding.scalar.generic is not None


# Defining a output-persist predicate to indicate if the copy of the instance should persist its output
def predicate(distributed_training_context):
    return (
        distributed_training_context[_sm_distribution.DistributedTrainingContextKey.CURRENT_HOST]
        == distributed_training_context[_sm_distribution.DistributedTrainingContextKey.HOSTS][1]
    )


def dontretry(f, *args, **kwargs):
    return f()


class DistributedCustomTrainingJobTaskTests(unittest.TestCase):
    @mock.patch.dict("os.environ", {})
    def setUp(self):
        with _utils.AutoDeletingTempDir("input_dir") as input_dir:
            self._task_input = _literals.LiteralMap(
                {"input_1": _literals.Literal(scalar=_literals.Scalar(primitive=_literals.Primitive(integer=1)))}
            )

            self._context = _common_engine.EngineContext(
                execution_id=WorkflowExecutionIdentifier(project="unit_test", domain="unit_test", name="unit_test"),
                execution_date=_datetime.datetime.utcnow(),
                stats=MockStats(),
                logging=None,
                tmp_dir=input_dir.name,
            )

            # Defining the distributed training task without specifying an output-persist
            # predicate (so it will use the default)
            @inputs(input_1=Types.Integer)
            @outputs(model=Types.Blob)
            @custom_training_job_task(
                training_job_resource_config=TrainingJobResourceConfig(
                    instance_type="ml.m4.xlarge", instance_count=2, volume_size_in_gb=25,
                ),
                algorithm_specification=AlgorithmSpecification(
                    input_mode=InputMode.FILE,
                    input_content_type=InputContentType.TEXT_CSV,
                    metric_definitions=[MetricDefinition(name="Validation error", regex="validation:error")],
                ),
            )
            def my_distributed_task(wf_params, input_1, model):
                pass

            self._my_distributed_task = my_distributed_task
            assert type(self._my_distributed_task) == CustomTrainingJobTask

    def test_missing_current_host_in_distributed_training_context_keys_lead_to_keyerrors(self):
        with mock.patch.dict(
            _os.environ,
            {
                _sm_distribution.SM_ENV_VAR_HOSTS: '["algo-0", "algo-1", "algo-2"]',
                _sm_distribution.SM_ENV_VAR_NETWORK_INTERFACE_NAME: "eth0",
            },
            clear=True,
        ):
            # eliminate the wait in unittest https://stackoverflow.com/a/32698175
            with mock.patch.object(retry.api, "__retry_internal", dontretry):
                self.assertRaises(KeyError, self._my_distributed_task.execute, self._context, self._task_input)

    def test_missing_hosts_distributed_training_context_keys_lead_to_keyerrors(self):
        with mock.patch.dict(
            _os.environ,
            {
                _sm_distribution.SM_ENV_VAR_CURRENT_HOST: "algo-1",
                _sm_distribution.SM_ENV_VAR_NETWORK_INTERFACE_NAME: "eth0",
            },
            clear=True,
        ):
            # eliminate the wait in unittest https://stackoverflow.com/a/32698175
            with mock.patch.object(retry.api, "__retry_internal", dontretry):
                self.assertRaises(KeyError, self._my_distributed_task.execute, self._context, self._task_input)

    def test_missing_network_interface_name_in_distributed_training_context_keys_lead_to_keyerrors(self):
        with mock.patch.dict(
            _os.environ,
            {
                _sm_distribution.SM_ENV_VAR_CURRENT_HOST: "algo-1",
                _sm_distribution.SM_ENV_VAR_HOSTS: '["algo-0", "algo-1", "algo-2"]',
            },
            clear=True,
        ):
            # eliminate the wait in unittest https://stackoverflow.com/a/32698175
            with mock.patch.object(retry.api, "__retry_internal", dontretry):
                self.assertRaises(KeyError, self._my_distributed_task.execute, self._context, self._task_input)

    def test_with_default_predicate_with_rank0_master(self):
        with mock.patch.dict(
            _os.environ,
            {
                _sm_distribution.SM_ENV_VAR_CURRENT_HOST: "algo-0",
                _sm_distribution.SM_ENV_VAR_HOSTS: '["algo-0", "algo-1", "algo-2"]',
                _sm_distribution.SM_ENV_VAR_NETWORK_INTERFACE_NAME: "eth0",
            },
            clear=True,
        ):
            # execute the distributed task with its distributed_training_context == None
            ret = self._my_distributed_task.execute(self._context, self._task_input)
            assert _common_constants.OUTPUT_FILE_NAME in ret.keys()

    def test_with_default_predicate_with_rank1_master(self):
        with mock.patch.dict(
            _os.environ,
            {
                _sm_distribution.SM_ENV_VAR_CURRENT_HOST: "algo-1",
                _sm_distribution.SM_ENV_VAR_HOSTS: '["algo-0", "algo-1", "algo-2"]',
                _sm_distribution.SM_ENV_VAR_NETWORK_INTERFACE_NAME: "eth0",
            },
            clear=True,
        ):
            ret = self._my_distributed_task.execute(self._context, self._task_input)
            assert not ret

    def test_with_custom_predicate_with_none_dist_context(self):
        with mock.patch.dict(
            _os.environ,
            {
                _sm_distribution.SM_ENV_VAR_CURRENT_HOST: "algo-1",
                _sm_distribution.SM_ENV_VAR_HOSTS: '["algo-0", "algo-1", "algo-2"]',
                _sm_distribution.SM_ENV_VAR_NETWORK_INTERFACE_NAME: "eth0",
            },
            clear=True,
        ):

            self._my_distributed_task._output_persist_predicate = predicate
            # execute the distributed task with its distributed_training_context == None
            ret = self._my_distributed_task.execute(self._context, self._task_input)
            assert ret
            assert _common_constants.OUTPUT_FILE_NAME in ret.keys()

    def test_with_custom_predicate_with_valid_dist_context(self):
        with mock.patch.dict(
            _os.environ,
            {
                _sm_distribution.SM_ENV_VAR_CURRENT_HOST: "algo-1",
                _sm_distribution.SM_ENV_VAR_HOSTS: '["algo-0", "algo-1", "algo-2"]',
                _sm_distribution.SM_ENV_VAR_NETWORK_INTERFACE_NAME: "eth0",
            },
            clear=True,
        ):
            # fill in the distributed_training_context to the context object and execute again
            self._my_distributed_task._output_persist_predicate = predicate
            ret = self._my_distributed_task.execute(self._context, self._task_input)
            assert _common_constants.OUTPUT_FILE_NAME in ret.keys()
            python_std_output_map = _type_helpers.unpack_literal_map_to_sdk_python_std(
                ret[_common_constants.OUTPUT_FILE_NAME]
            )
            assert "model" in python_std_output_map.keys()

    def test_if_wf_param_has_dist_context(self):
        with mock.patch.dict(
            _os.environ,
            {
                _sm_distribution.SM_ENV_VAR_CURRENT_HOST: "algo-1",
                _sm_distribution.SM_ENV_VAR_HOSTS: '["algo-0", "algo-1", "algo-2"]',
                _sm_distribution.SM_ENV_VAR_NETWORK_INTERFACE_NAME: "eth0",
            },
            clear=True,
        ):

            # This test is making sure that the distributed_training_context is successfully passed into the
            # task_function.
            # Specifically, we want to make sure the _execute_user_code() of the CustomTrainingJobTask class does the
            # thing that it is supposed to do

            @inputs(input_1=Types.Integer)
            @outputs(model=Types.Blob)
            @custom_training_job_task(
                training_job_resource_config=TrainingJobResourceConfig(
                    instance_type="ml.m4.xlarge", instance_count=2, volume_size_in_gb=25,
                ),
                algorithm_specification=AlgorithmSpecification(
                    input_mode=InputMode.FILE,
                    input_content_type=InputContentType.TEXT_CSV,
                    metric_definitions=[MetricDefinition(name="Validation error", regex="validation:error")],
                ),
            )
            def my_distributed_task_with_valid_dist_training_context(wf_params, input_1, model):
                if not wf_params.distributed_training_context:
                    raise ValueError

            try:
                my_distributed_task_with_valid_dist_training_context.execute(self._context, self._task_input)
            except ValueError:
                self.fail("The distributed_training_context is not passed into task function successfully")


class HPOOnCustomTrainingJobTaskTests(unittest.TestCase):
    def test_a(self):
        @inputs(input_1=Types.Integer)
        @outputs(model=Types.Blob)
        @custom_training_job_task(
            training_job_resource_config=TrainingJobResourceConfig(
                instance_type="ml.m4.xlarge", instance_count=2, volume_size_in_gb=25,
            ),
            algorithm_specification=AlgorithmSpecification(
                input_mode=InputMode.FILE,
                input_content_type=InputContentType.TEXT_CSV,
                metric_definitions=[MetricDefinition(name="Validation error", regex="validation:error")],
            ),
        )
        def my_distributed_task_with_valid_dist_training_context(wf_params, input_1, model):
            if not wf_params.distributed_training_context:
                raise ValueError

        hpo_task = SdkSimpleHyperparameterTuningJobTask(
            training_job=my_distributed_task_with_valid_dist_training_context,
            max_parallel_training_jobs=5,
            max_number_of_training_jobs=10,
            tunable_parameters=['input_1'],
        )
