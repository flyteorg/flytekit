import datetime

import pytest

from flytekit import __version__
from flytekit.core.base_task import TaskMetadata
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_model


def test_post_init_conditions():
    with pytest.raises(ValueError, match="Caching is enabled ``cache=True`` but ``cache_version`` is not set."):
        TaskMetadata(cache=True, cache_version="")

    with pytest.raises(
        ValueError, match="Cache serialize is enabled ``cache_serialize=True`` but ``cache`` is not enabled."
    ):
        TaskMetadata(cache=False, cache_serialize=True)

    with pytest.raises(
        ValueError, match="timeout should be duration represented as either a datetime.timedelta or int seconds"
    ):
        TaskMetadata(timeout="invalid_timeout")

    tm = TaskMetadata(timeout=3600)
    assert isinstance(tm.timeout, datetime.timedelta)


def test_retry_strategy():
    tm = TaskMetadata(retries=5)
    assert tm.retry_strategy.retries == 5


def test_to_taskmetadata_model():
    tm = TaskMetadata(
        cache=True,
        cache_serialize=True,
        cache_version="v1",
        interruptible=True,
        deprecated="TEST DEPRECATED ERROR MESSAGE",
        retries=3,
        timeout=3600,
        pod_template_name="TEST POD TEMPLATE NAME",
        runtime_flavor="sync_plugin",
    )
    model = tm.to_taskmetadata_model()

    assert model.discoverable is True
    assert model.runtime == _task_model.RuntimeMetadata(
        _task_model.RuntimeMetadata.RuntimeType.FLYTE_SDK,
        __version__,
        "sync_plugin",
    )
    assert model.retries == _literal_models.RetryStrategy(3)
    assert model.timeout == datetime.timedelta(seconds=3600)
    assert model.interruptible is True
    assert model.discovery_version == "v1"
    assert model.deprecated_error_message == "TEST DEPRECATED ERROR MESSAGE"
    assert model.cache_serializable is True
    assert model.pod_template_name == "TEST POD TEMPLATE NAME"
