from __future__ import absolute_import
from flytekit.sdk.tasks import pytorch_task, inputs, outputs
from flytekit.sdk.types import Types
from flytekit.common import constants as _common_constants
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.tasks import sagemaker_task as _sagemaker_task
from flytekit.models import types as _type_models
from flytekit.models.core import identifier as _identifier
import datetime as _datetime


@inputs(
    train_data=Types.Blob,
    validate_data=Types.Blob,
)
@outputs(
    model=Types.Blob
)
@trainingjob_task(trainingjob_conf={'A': 'B'})
def default_trainingjob_task(wf_params, tjconf, train_data, validate_data, model):
    pass    

default_trainingjob_task._id = _identifier.Identifier(
    _identifier.ResourceType.TASK, "my_project", "my_domain", "my_name", "my_version")


def test_default_trainingjob_task():
    assert isinstance(default_trainingjob_task, _sagemaker_task.SdkTrainingJobTask)
    assert isinstance(default_trainingjob_task, _sdk_runnable.SdkRunnableTask)
    assert default_trainingjob_task.interface.inputs['train_data'].description == ''
    assert default_trainingjob_task.interface.inputs['train_data'].type == \
           _type_models.LiteralType(schema=_type_models.SchemaType)
    assert default_trainingjob_task.interface.outputs['model'].description == ''
    assert default_trainingjob_task.interface.outputs['model'].type == \
           _type_models.LiteralType(blob=_type_models.LiteralType.blob)
    assert default_trainingjob_task.type == _common_constants.SdkTaskType.SAGEMAKER_TRAININGJOB_TASK
    assert default_trainingjob_task.task_function_name == 'default_trainingjob_task'
    assert default_trainingjob_task.task_module == __name__
    assert default_trainingjob_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert default_trainingjob_task.metadata.deprecated_error_message == ''
    assert default_trainingjob_task.metadata.discoverable is False
    assert default_trainingjob_task.metadata.discovery_version == ''
    assert default_trainingjob_task.metadata.retries.retries == 0
    assert len(default_trainingjob_task.container.resources.limits) == 0
    assert len(default_trainingjob_task.container.resources.requests) == 0
    assert default_trainingjob_task.custom['trainingjobConf']['A'] == 'B'
    assert default_trainingjob_task._get_container_definition().args[0] == 'pyflyte-execute'

    pb2 = default_trainingjob_task.to_flyte_idl()
    assert pb2.custom['trainingjobConf']['A'] == 'B'


@inputs(
    hyperparameter_ranges=Types.Generic,
)
@outputs(
    model=Types.Blob
)
@hpojob_task(hpojob_conf={'C': 'D'})
def default_hpojob_task(wf_params, hjconf, hyperparameter_ranges, model):
    pass

default_hpojob_task._id = _identifier.Identifier(
    _identifier.ResourceType.TASK, "my_project", "my_domain", "my_name", "my_version")


def test_default_trainingjob_task():
    assert isinstance(default_hpojob_task, _sagemaker_task.SdkHPOJobTask)
    assert isinstance(default_hpojob_task, _sdk_runnable.SdkRunnableTask)
    assert default_hpojob_task.interface.inputs['hyperparameter_ranges'].description == ''
    assert default_hpojob_task.interface.inputs['hyperparameter_ranges'].type == \
           _type_models.LiteralType(schema=_type_models.SchemaType)
    assert default_hpojob_task.interface.outputs['model'].description == ''
    assert default_hpojob_task.interface.outputs['model'].type == \
           _type_models.LiteralType(blob=_type_models.LiteralType.blob)
    assert default_hpojob_task.type == _common_constants.SdkTaskType.SAGEMAKER_HPOJOB_TASK
    assert default_hpojob_task.task_function_name == 'default_hpojob_task'
    assert default_hpojob_task.task_module == __name__
    assert default_hpojob_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert default_hpojob_task.metadata.deprecated_error_message == ''
    assert default_hpojob_task.metadata.discoverable is False
    assert default_hpojob_task.metadata.discovery_version == ''
    assert default_hpojob_task.metadata.retries.retries == 0
    assert len(default_hpojob_task.container.resources.limits) == 0
    assert len(default_hpojob_task.container.resources.requests) == 0
    assert default_hpojob_task.custom['hpojobConf']['C'] == 'D'
    assert default_hpojob_task._get_container_definition().args[0] == 'pyflyte-execute'

    pb2 = default_hpojob_task.to_flyte_idl()
    assert pb2.custom['hpojobConf']['C'] == 'D'
