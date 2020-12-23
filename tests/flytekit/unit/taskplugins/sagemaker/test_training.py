import os
import tempfile
from typing import TypeVar

import pytest

from flytekit import task, metadata, kwtypes
from flytekit.annotated.context_manager import RegistrationSettings, Image, ImageConfig
from flytekit.taskplugins.sagemaker import SagemakerTrainingJobConfig, SagemakerBuiltinAlgorithmsTask, \
    TrainingJobResourceConfig, AlgorithmSpecification, AlgorithmName
from flytekit.types import FlyteFile


def _get_reg_settings():
    default_img = Image(name="default", fqn="test", tag="tag")
    reg = RegistrationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    return reg


def test_builtin_training():
    trainer = SagemakerBuiltinAlgorithmsTask(
        name="builtin-trainer",
        metadata=metadata(),
        task_config=SagemakerTrainingJobConfig(
            training_job_resource_config=TrainingJobResourceConfig(
                instance_count=1,
                instance_type="ml-xlarge",
                volume_size_in_gb=1,
            ),
            algorithm_specification=AlgorithmSpecification(
                algorithm_name=AlgorithmName.XGBOOST,
            )
        ),
    )

    assert trainer.python_interface.inputs.keys() == {"static_hyperparameters", "train", "validation"}
    assert trainer.python_interface.outputs.keys() == {"model"}

    with tempfile.TemporaryDirectory() as tmp:
        x = os.path.join(tmp, "x")
        y = os.path.join(tmp, "y")
        with open(x, "w") as f:
            f.write("test")
        with open(y, "w") as f:
            f.write("test")
        with pytest.raises(NotImplementedError):
            trainer(static_hyperparameters={}, train=x, validation=y)

    assert trainer.get_custom(_get_reg_settings()) == {'algorithmSpecification': {'algorithmName': 'XGBOOST'},
                                                       'trainingJobResourceConfig': {'instanceCount': '1',
                                                                                     'instanceType': 'ml-xlarge',
                                                                                     'volumeSizeInGb': '1'},
                                                       }


def test_custom_training():
    @task(task_config=SagemakerTrainingJobConfig(
        training_job_resource_config=TrainingJobResourceConfig(
            instance_count=1,
            instance_type="ml-xlarge",
            volume_size_in_gb=1,
        ),
        algorithm_specification=AlgorithmSpecification(
            algorithm_name=AlgorithmName.CUSTOM,
        )))
    def my_custom_trainer(x: int) -> int:
        return x

    assert my_custom_trainer.python_interface.inputs == {"x": int}
    assert my_custom_trainer.python_interface.outputs == {"out_0": int}

    assert my_custom_trainer(x=10) == 10

    assert my_custom_trainer.get_custom(_get_reg_settings()) == {
        'algorithmSpecification': {},
        'trainingJobResourceConfig': {'instanceCount': '1',
                                      'instanceType': 'ml-xlarge',
                                      'volumeSizeInGb': '1'},
    }
