import pytest
from flytekitplugins.awssagemaker.hpo import (
    HPOJob,
    HPOTuningJobConfigTransformer,
    ParameterRangesTransformer,
    SagemakerHPOTask,
)
from flytekitplugins.awssagemaker.training import SagemakerBuiltinAlgorithmsTask, SagemakerTrainingJobConfig

from flytekit import FlyteContext
from flytekit.common.types.primitives import Generic
from flytekit.models.sagemaker.hpo_job import (
    HyperparameterTuningJobConfig,
    HyperparameterTuningObjective,
    HyperparameterTuningObjectiveType,
    TrainingJobEarlyStoppingType,
)
from flytekit.models.sagemaker.parameter_ranges import IntegerParameterRange, ParameterRangeOneOf
from flytekit.models.sagemaker.training_job import AlgorithmName, AlgorithmSpecification, TrainingJobResourceConfig
from plugins.tests.awssagemaker.test_training import _get_reg_settings


def test_hpo_for_builtin():
    trainer = SagemakerBuiltinAlgorithmsTask(
        name="builtin-trainer",
        task_config=SagemakerTrainingJobConfig(
            training_job_resource_config=TrainingJobResourceConfig(
                instance_count=1,
                instance_type="ml-xlarge",
                volume_size_in_gb=1,
            ),
            algorithm_specification=AlgorithmSpecification(
                algorithm_name=AlgorithmName.XGBOOST,
            ),
        ),
    )

    hpo = SagemakerHPOTask(
        name="test",
        task_config=HPOJob(10, 10, ["x"]),
        training_task=trainer,
    )

    assert hpo.python_interface.inputs.keys() == {
        "static_hyperparameters",
        "train",
        "validation",
        "hyperparameter_tuning_job_config",
        "x",
    }
    assert hpo.python_interface.outputs.keys() == {"model"}

    assert hpo.get_custom(_get_reg_settings()) == {
        "maxNumberOfTrainingJobs": "10",
        "maxParallelTrainingJobs": "10",
        "trainingJob": {
            "algorithmSpecification": {"algorithmName": "XGBOOST"},
            "trainingJobResourceConfig": {"instanceCount": "1", "instanceType": "ml-xlarge", "volumeSizeInGb": "1"},
        },
    }

    with pytest.raises(NotImplementedError):
        hpo(
            static_hyperparameters={},
            train="",
            validation="",
            hyperparameter_tuning_job_config=HyperparameterTuningJobConfig(
                tuning_strategy=1,
                tuning_objective=HyperparameterTuningObjective(
                    objective_type=HyperparameterTuningObjectiveType.MINIMIZE,
                    metric_name="x",
                ),
                training_job_early_stopping_type=TrainingJobEarlyStoppingType.OFF,
            ),
            x=ParameterRangeOneOf(param=IntegerParameterRange(10, 1, 1)),
        )


def test_hpoconfig_transformer():
    t = HPOTuningJobConfigTransformer()
    assert t.get_literal_type(HyperparameterTuningJobConfig) == Generic.to_flyte_literal_type()
    o = HyperparameterTuningJobConfig(
        tuning_strategy=1,
        tuning_objective=HyperparameterTuningObjective(
            objective_type=HyperparameterTuningObjectiveType.MINIMIZE,
            metric_name="x",
        ),
        training_job_early_stopping_type=TrainingJobEarlyStoppingType.OFF,
    )
    ctx = FlyteContext.current_context()
    lit = t.to_literal(ctx, python_val=o, python_type=HyperparameterTuningJobConfig, expected=None)
    assert lit is not None
    assert lit.scalar.generic is not None
    ro = t.to_python_value(ctx, lit, HyperparameterTuningJobConfig)
    assert ro is not None
    assert ro == o


def test_parameter_ranges_transformer():
    t = ParameterRangesTransformer()
    assert t.get_literal_type(ParameterRangeOneOf) == Generic.to_flyte_literal_type()
    o = ParameterRangeOneOf(param=IntegerParameterRange(10, 0, 1))
    ctx = FlyteContext.current_context()
    lit = t.to_literal(ctx, python_val=o, python_type=ParameterRangeOneOf, expected=None)
    assert lit is not None
    assert lit.scalar.generic is not None
    ro = t.to_python_value(ctx, lit, ParameterRangeOneOf)
    assert ro is not None
    assert ro == o
