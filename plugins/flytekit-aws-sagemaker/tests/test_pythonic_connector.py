"""Unit tests for the Pythonic-mode SageMaker connectors (Processing + Training)
and their shared base (request building, outputs.pb/error.pb resolution, job
name generation, idempotency, stop-on-delete)."""

import types
from collections import OrderedDict
from pathlib import Path
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from flyteidl.core import errors_pb2
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.awssagemaker_inference.boto3_mixin import CustomException
from flytekitplugins.awssagemaker_inference.pythonic_base import (
    PythonicJobMetadata,
    _PythonicJobError,
    _make_job_name,
    _tags_to_list,
)
from flytekitplugins.awssagemaker_processing import SageMakerProcessing
from flytekitplugins.awssagemaker_training import SageMakerTraining

from flytekit import task
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.constants import FLYTE_FAIL_ON_ERROR
from flytekit.extend.backend.base_connector import ConnectorRegistry
from flytekit.extend.backend.utils import render_task_template
from flytekit.tools.translator import get_serializable

ROLE = "arn:aws:iam::123456789012:role/sm-exec"
REGION = "us-east-1"
OUTPUT_PREFIX = "s3://bucket/flyte/exec/n0"
IMAGE = "123.dkr.ecr.us-east-1.amazonaws.com/img:tag"
ARGS = ["pyflyte-fast-execute", "--", "pyflyte-execute", "--output-prefix", OUTPUT_PREFIX]

_CALL = "flytekitplugins.awssagemaker_inference.boto3_mixin.Boto3ConnectorMixin._call"
_READ_ERROR = (
    "flytekitplugins.awssagemaker_inference.pythonic_base."
    "PythonicSageMakerJobConnector._read_error"
)
_ARTIFACT_EXISTS = (
    "flytekitplugins.awssagemaker_inference.pythonic_base."
    "PythonicSageMakerJobConnector._artifact_exists"
)


def _template(custom, args=ARGS, image=IMAGE, env=None, outputs=None):
    container = types.SimpleNamespace(image=image, args=args, env=env or {})
    tid = types.SimpleNamespace(
        project="project",
        domain="domain",
        name="project.domain.my_task",
        version="v1",
    )
    interface = types.SimpleNamespace(outputs=outputs or {})
    return types.SimpleNamespace(
        container=container,
        custom=custom,
        id=tid,
        interface=interface,
    )


def _client_error(code, message, op):
    return CustomException(
        message="boom",
        idempotence_token="tok",
        original_exception=ClientError(
            error_response={"Error": {"Code": code, "Message": message}},
            operation_name=op,
        ),
    )


# --------------------------- shared base helpers ---------------------------


def test_tags_to_list():
    assert _tags_to_list(None) is None
    assert _tags_to_list({}) is None
    assert _tags_to_list({"a": "b"}) == [{"Key": "a", "Value": "b"}]


def test_metadata_encode_decode_round_trip():
    meta = PythonicJobMetadata(
        job_name="j",
        output_prefix=OUTPUT_PREFIX,
        region=REGION,
        has_outputs=True,
    )
    assert PythonicJobMetadata.decode(meta.encode()) == meta


def test_make_job_name_valid_and_unique_per_retry():
    tt = _template({})
    md_retry0 = types.SimpleNamespace(
        task_execution_id=types.SimpleNamespace(
            node_execution_id=types.SimpleNamespace(
                execution_id=types.SimpleNamespace(name="exec1"), node_id="n0"
            ),
            retry_attempt=0,
        )
    )
    md_retry1 = types.SimpleNamespace(
        task_execution_id=types.SimpleNamespace(
            node_execution_id=types.SimpleNamespace(
                execution_id=types.SimpleNamespace(name="exec1"), node_id="n0"
            ),
            retry_attempt=1,
        )
    )
    name0 = _make_job_name("flyte-", md_retry0, tt)
    name1 = _make_job_name("flyte-", md_retry1, tt)

    assert name0 != name1  # retries get distinct names
    for name in (name0, name1):
        assert name.startswith("flyte-")
        assert len(name) <= 63
        assert name[0].isalnum()
        assert all(c.isalnum() or c == "-" for c in name)


def test_make_job_name_falls_back_to_task_id_without_metadata():
    name = _make_job_name("flyte-", None, _template({}))
    assert name.startswith("flyte-")
    assert len(name) <= 63


def test_make_job_name_reserves_digest_for_long_prefixes():
    first = _make_job_name("x" * 100, None, _template({}), "s3://bucket/exec-1")
    second = _make_job_name("x" * 100, None, _template({}), "s3://bucket/exec-2")

    assert len(first) <= 63
    assert len(second) <= 63
    assert first != second


def test_read_error_preserves_recoverable_kind(monkeypatch, tmp_path):
    import flytekitplugins.awssagemaker_inference.pythonic_base as base

    payload = errors_pb2.ErrorDocument(
        error=errors_pb2.ContainerError(
            code="USER:Recoverable",
            message="try again",
            kind=errors_pb2.ContainerError.RECOVERABLE,
        )
    ).SerializeToString()
    local_path = tmp_path / "error.pb"
    file_access = types.SimpleNamespace(
        exists=lambda _path: True,
        get_random_local_path=lambda: str(local_path),
        get_data=lambda _remote, target: Path(target).write_bytes(payload),
    )
    monkeypatch.setattr(
        base.FlyteContext,
        "current_context",
        lambda: types.SimpleNamespace(file_access=file_access),
    )

    error = base.PythonicSageMakerJobConnector._read_error(OUTPUT_PREFIX)

    assert error == _PythonicJobError(message="try again", recoverable=True)


def test_read_error_propagates_storage_failures(monkeypatch):
    import flytekitplugins.awssagemaker_inference.pythonic_base as base

    file_access = types.SimpleNamespace(
        exists=mock.Mock(side_effect=PermissionError("S3 access denied"))
    )
    monkeypatch.setattr(
        base.FlyteContext,
        "current_context",
        lambda: types.SimpleNamespace(file_access=file_access),
    )

    with pytest.raises(PermissionError, match="S3 access denied"):
        base.PythonicSageMakerJobConnector._read_error(OUTPUT_PREFIX)


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_real_serialized_task_renders_entrypoint_and_environment(mock_call):
    @task(
        task_config=SageMakerProcessing(execution_role_arn=ROLE, region=REGION),
        container_image=IMAGE,
        environment={"FROM_TASK": "yes"},
    )
    def serialized_processing(value: int) -> int:
        return value + 1

    default_image = Image(name="default", fqn=IMAGE.rsplit(":", 1)[0], tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        image_config=ImageConfig(
            default_image=default_image,
            images=[default_image],
        ),
    )
    task_spec = get_serializable(OrderedDict(), settings, serialized_processing)
    template = render_task_template(task_spec.template, OUTPUT_PREFIX)
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    mock_call.return_value = ({}, "tok")

    metadata = await connector.create(template, output_prefix=OUTPUT_PREFIX)

    request = mock_call.call_args.kwargs["config"]
    entrypoint = request["AppSpecification"]["ContainerEntrypoint"]
    assert any(OUTPUT_PREFIX in argument for argument in entrypoint)
    assert not any("{{." in argument for argument in entrypoint)
    assert request["Environment"]["FROM_TASK"] == "yes"
    assert request["Environment"][FLYTE_FAIL_ON_ERROR] == "true"
    assert metadata.has_outputs is True


# --------------------------- processing connector ---------------------------


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_processing_create_builds_request(mock_call):
    mock_call.return_value = ({}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    custom = SageMakerProcessing(
        execution_role_arn=ROLE,
        region=REGION,
        instance_type="ml.m5.xlarge",
        tags={"team": "ml"},
        environment={"K": "V"},
    ).to_dict()

    meta = await connector.create(
        _template(
            custom,
            env={"FROM_TASK": "yes", "K": "old"},
            outputs={"result": object()},
        ),
        output_prefix=OUTPUT_PREFIX,
    )

    assert meta.output_prefix == OUTPUT_PREFIX
    assert meta.region == REGION
    assert meta.has_outputs is True
    assert meta.job_name.startswith("flyte-")

    req = mock_call.call_args.kwargs["config"]
    assert mock_call.call_args.kwargs["method"] == "create_processing_job"
    assert req["ProcessingJobName"] == meta.job_name
    assert req["RoleArn"] == ROLE
    assert req["AppSpecification"] == {"ImageUri": IMAGE, "ContainerEntrypoint": ARGS}
    cc = req["ProcessingResources"]["ClusterConfig"]
    assert cc["InstanceType"] == "ml.m5.xlarge"
    assert cc["InstanceCount"] == 1
    assert req["Environment"] == {
        "FROM_TASK": "yes",
        "K": "V",
        FLYTE_FAIL_ON_ERROR: "true",
    }
    assert req["Tags"] == [{"Key": "team", "Value": "ml"}]


@pytest.mark.asyncio
@mock.patch(_READ_ERROR, return_value=None)
@mock.patch(_CALL)
async def test_processing_get_completed_no_error_succeeds(mock_call, _mock_err):
    mock_call.return_value = ({"ProcessingJobStatus": "Completed"}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX, region=REGION)

    resource = await connector.get(meta)
    assert resource.phase == TaskExecution.SUCCEEDED
    # outputs=None -> flytekit materializes the typed return from outputs.pb.
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch(_READ_ERROR, return_value=_PythonicJobError("ValueError: boom", False))
@mock.patch(_CALL)
async def test_processing_get_completed_with_error_pb_fails(mock_call, _mock_err):
    mock_call.return_value = ({"ProcessingJobStatus": "Completed"}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX, region=REGION)

    resource = await connector.get(meta)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "ValueError: boom"


@pytest.mark.asyncio
@mock.patch(_READ_ERROR, return_value=_PythonicJobError("temporary failure", True))
@mock.patch(_CALL)
async def test_processing_recoverable_error_is_retryable(mock_call, _mock_err):
    mock_call.return_value = ({"ProcessingJobStatus": "Failed"}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX, region=REGION)

    resource = await connector.get(meta)

    assert resource.phase == TaskExecution.RETRYABLE_FAILED
    assert resource.message == "temporary failure"


@pytest.mark.asyncio
@mock.patch(_ARTIFACT_EXISTS, return_value=False)
@mock.patch(_READ_ERROR, return_value=None)
@mock.patch(_CALL)
async def test_processing_completed_without_declared_outputs_fails(
    mock_call,
    _mock_err,
    _mock_exists,
):
    mock_call.return_value = ({"ProcessingJobStatus": "Completed"}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    meta = PythonicJobMetadata(
        job_name="j",
        output_prefix=OUTPUT_PREFIX,
        region=REGION,
        has_outputs=True,
    )

    resource = await connector.get(meta)

    assert resource.phase == TaskExecution.FAILED
    assert "without producing Flyte outputs.pb" in resource.message


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_processing_get_in_progress_running(mock_call):
    mock_call.return_value = ({"ProcessingJobStatus": "InProgress"}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX)

    resource = await connector.get(meta)
    assert resource.phase == TaskExecution.RUNNING
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_processing_create_already_exists_is_idempotent(mock_call):
    mock_call.side_effect = _client_error(
        "ResourceInUse", "Processing job flyte-x already exists", "CreateProcessingJob"
    )
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    meta = await connector.create(
        _template(SageMakerProcessing(execution_role_arn=ROLE, region=REGION).to_dict()),
        output_prefix=OUTPUT_PREFIX,
    )
    assert meta.output_prefix == OUTPUT_PREFIX


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_processing_create_resource_limit_propagates(mock_call):
    mock_call.side_effect = _client_error(
        "ResourceLimitExceeded",
        "Processing job quota exceeded",
        "CreateProcessingJob",
    )
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    with pytest.raises(CustomException):
        await connector.create(
            _template(SageMakerProcessing(execution_role_arn=ROLE, region=REGION).to_dict()),
            output_prefix=OUTPUT_PREFIX,
        )


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_processing_create_unknown_error_propagates(mock_call):
    mock_call.side_effect = _client_error("AccessDeniedException", "nope", "CreateProcessingJob")
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    with pytest.raises(CustomException):
        await connector.create(
            _template(SageMakerProcessing(execution_role_arn=ROLE, region=REGION).to_dict()),
            output_prefix=OUTPUT_PREFIX,
        )


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_processing_delete_stops_job(mock_call):
    mock_call.return_value = ({}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX, region=REGION)
    assert await connector.delete(meta) is None
    assert mock_call.call_args.kwargs["method"] == "stop_processing_job"


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_processing_delete_swallows_non_running(mock_call):
    mock_call.side_effect = _client_error(
        "ValidationException", "the processing job is not in a non-running state", "StopProcessingJob"
    )
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX)
    assert await connector.delete(meta) is None


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_processing_delete_swallows_resource_not_found(mock_call):
    mock_call.side_effect = _client_error(
        "ResourceNotFound",
        "Processing job does not exist",
        "StopProcessingJob",
    )
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX, region=REGION)
    assert await connector.delete(meta) is None


# --------------------------- training connector ---------------------------


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_training_create_builds_request(mock_call):
    mock_call.return_value = ({}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-training-task")
    custom = SageMakerTraining(
        execution_role_arn=ROLE,
        region=REGION,
        instance_type="ml.m5.large",
        vpc_config={"Subnets": ["subnet-1"], "SecurityGroupIds": ["sg-1"]},
    ).to_dict()

    meta = await connector.create(_template(custom), output_prefix=OUTPUT_PREFIX)

    req = mock_call.call_args.kwargs["config"]
    assert mock_call.call_args.kwargs["method"] == "create_training_job"
    assert req["TrainingJobName"] == meta.job_name
    assert req["RoleArn"] == ROLE
    assert req["AlgorithmSpecification"] == {
        "TrainingImage": IMAGE,
        "ContainerEntrypoint": ARGS,
        "TrainingInputMode": "File",
    }
    assert req["ResourceConfig"]["InstanceType"] == "ml.m5.large"
    # OutputDataConfig defaults to the Flyte output prefix when unset.
    assert req["OutputDataConfig"]["S3OutputPath"] == f"{OUTPUT_PREFIX}/_sagemaker_model"
    assert req["VpcConfig"] == {"Subnets": ["subnet-1"], "SecurityGroupIds": ["sg-1"]}
    assert req["Environment"][FLYTE_FAIL_ON_ERROR] == "true"


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_training_create_honours_explicit_output_path(mock_call):
    mock_call.return_value = ({}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-training-task")
    custom = SageMakerTraining(
        execution_role_arn=ROLE,
        region=REGION,
        output_s3_path="s3://b/model/",
    ).to_dict()
    await connector.create(_template(custom), output_prefix=OUTPUT_PREFIX)
    req = mock_call.call_args.kwargs["config"]
    assert req["OutputDataConfig"]["S3OutputPath"] == "s3://b/model/"


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_training_get_in_progress_surfaces_secondary_status(mock_call):
    mock_call.return_value = (
        {"TrainingJobStatus": "InProgress", "SecondaryStatus": "Downloading"},
        "tok",
    )
    connector = ConnectorRegistry.get_connector("sagemaker-training-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX)
    resource = await connector.get(meta)
    assert resource.phase == TaskExecution.RUNNING
    assert resource.message == "Downloading"


@pytest.mark.asyncio
@mock.patch(_READ_ERROR, return_value=None)
@mock.patch(_CALL)
async def test_training_get_completed_no_error_succeeds(mock_call, _mock_err):
    mock_call.return_value = ({"TrainingJobStatus": "Completed"}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-training-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX)
    resource = await connector.get(meta)
    assert resource.phase == TaskExecution.SUCCEEDED
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch(_READ_ERROR, return_value=_PythonicJobError("RuntimeError: nan loss", False))
@mock.patch(_CALL)
async def test_training_get_completed_with_error_pb_fails(mock_call, _mock_err):
    mock_call.return_value = ({"TrainingJobStatus": "Completed"}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-training-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX)
    resource = await connector.get(meta)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "RuntimeError: nan loss"


@pytest.mark.asyncio
@mock.patch(_READ_ERROR, return_value=None)
@mock.patch(_CALL)
async def test_training_get_failed_surfaces_failure_reason(mock_call, _mock_err):
    mock_call.return_value = (
        {"TrainingJobStatus": "Failed", "FailureReason": "ClientError: boom"},
        "tok",
    )
    connector = ConnectorRegistry.get_connector("sagemaker-training-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX)
    resource = await connector.get(meta)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "ClientError: boom"


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_training_delete_stops_job(mock_call):
    mock_call.return_value = ({}, "tok")
    connector = ConnectorRegistry.get_connector("sagemaker-training-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX)
    assert await connector.delete(meta) is None
    assert mock_call.call_args.kwargs["method"] == "stop_training_job"


@pytest.mark.asyncio
@mock.patch(_CALL)
async def test_training_delete_swallows_resource_not_found(mock_call):
    mock_call.side_effect = _client_error(
        "ResourceNotFound",
        "Training job does not exist",
        "StopTrainingJob",
    )
    connector = ConnectorRegistry.get_connector("sagemaker-training-task")
    meta = PythonicJobMetadata(job_name="j", output_prefix=OUTPUT_PREFIX, region=REGION)
    assert await connector.delete(meta) is None


# --------------------------- container and network guards ---------------------------


@pytest.mark.asyncio
async def test_create_without_container_image_raises():
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    tt = _template(
        SageMakerProcessing(execution_role_arn=ROLE, region=REGION).to_dict(),
        image=None,
    )
    with pytest.raises(ValueError):
        await connector.create(tt, output_prefix=OUTPUT_PREFIX)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args,error",
    [
        ([], "rendered Flyte container entrypoint"),
        (["x"] * 101, "at most 100 arguments"),
        (["x" * 257], "at most 256 characters"),
    ],
)
async def test_create_rejects_invalid_container_entrypoint(args, error):
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    tt = _template(
        SageMakerProcessing(execution_role_arn=ROLE, region=REGION).to_dict(),
        args=args,
    )
    with pytest.raises(ValueError, match=error):
        await connector.create(tt, output_prefix=OUTPUT_PREFIX)


@pytest.mark.asyncio
async def test_processing_rejects_network_isolation():
    connector = ConnectorRegistry.get_connector("sagemaker-processing-task")
    config = SageMakerProcessing(
        execution_role_arn=ROLE,
        region=REGION,
        network_config={"EnableNetworkIsolation": True},
    ).to_dict()
    with pytest.raises(ValueError, match="incompatible with Pythonic mode"):
        await connector.create(_template(config), output_prefix=OUTPUT_PREFIX)
