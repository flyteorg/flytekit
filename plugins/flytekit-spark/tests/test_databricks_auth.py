"""Tests for the Databricks authentication strategy boundary."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from flytekitplugins.spark.databricks_auth import (
    PATAuth,
    _Settings,
    select_auth,
)


def _task_template(**custom):
    task_template = MagicMock()
    task_template.custom = custom
    return task_template


def test_settings_use_default_secret_name():
    settings = _Settings.from_task(task_template=None, namespace="project-a")

    assert settings.token_secret_name is None
    assert settings.namespace == "project-a"


def test_settings_use_task_secret_name():
    task_template = _task_template()
    task_template.custom["databricksTokenSecret"] = "custom-token"

    settings = _Settings.from_task(task_template=task_template, namespace="project-a")

    assert settings.token_secret_name == "custom-token"


@pytest.mark.asyncio
async def test_select_auth_returns_pat_by_default():
    task_template = _task_template()
    auth = await select_auth(task_template=task_template, namespace="project-a")

    assert isinstance(auth, PATAuth)
    assert auth.auth_type == "pat"


@pytest.mark.asyncio
async def test_pat_auth_delegates_to_existing_token_lookup():
    task_template = _task_template()
    settings = _Settings(
        task_template=task_template,
        token_secret_name="custom-token",
        namespace="project-a",
    )
    auth = PATAuth(settings)

    with patch(
        "flytekitplugins.spark.connector.get_databricks_token",
        return_value="example-token",
    ) as get_token:
        token = await auth.get_bearer_token(AsyncMock())

    assert token == "example-token"
    get_token.assert_called_once_with(
        namespace="project-a",
        task_template=task_template,
        secret_name="custom-token",
    )
