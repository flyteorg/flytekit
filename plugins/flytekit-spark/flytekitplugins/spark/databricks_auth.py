"""Authentication strategies for the Databricks connector.

This module introduces a small strategy boundary around the connector's
existing Personal Access Token (PAT) flow. It intentionally preserves the
current token resolution behavior so additional authentication methods can be
added independently.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from flytekit import lazy_module
from flytekit.models.task import TaskTemplate

aiohttp = lazy_module("aiohttp")


@dataclass
class _Settings:
    """Authentication settings resolved for one Databricks task."""

    task_template: Optional[TaskTemplate]
    token_secret_name: Optional[str]
    namespace: Optional[str]

    @staticmethod
    def from_task(task_template: Optional[TaskTemplate], namespace: Optional[str]) -> "_Settings":
        custom = task_template.custom if task_template is not None else {}
        return _Settings(
            task_template=task_template,
            token_secret_name=custom.get("databricksTokenSecret"),
            namespace=namespace,
        )


class DatabricksAuth(ABC):
    """Interface for obtaining a bearer token for Databricks API calls."""

    auth_type = "unknown"
    strategy_name = "DatabricksAuth"

    def __init__(self, settings: _Settings):
        self.settings = settings

    @abstractmethod
    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        """Return a bearer token for a Databricks API request."""

    def describe(self) -> str:
        """Return a description that is safe to write to connector logs."""
        return (
            f"strategy={self.strategy_name} auth_type={self.auth_type} " f"namespace={self.settings.namespace or 'N/A'}"
        )


class PATAuth(DatabricksAuth):
    """Delegate to the connector's existing multi-tenant PAT lookup."""

    auth_type = "pat"
    strategy_name = "PATAuth"

    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        from .connector import get_databricks_token

        return get_databricks_token(
            namespace=self.settings.namespace,
            task_template=self.settings.task_template,
            secret_name=self.settings.token_secret_name,
        )


async def select_auth(
    task_template: Optional[TaskTemplate],
    namespace: Optional[str],
) -> DatabricksAuth:
    """Select authentication for a task.

    PAT remains the only strategy and therefore the unconditional default in
    this refactor. Later authentication methods can extend this dispatcher
    without changing the connector request lifecycle.
    """
    return PATAuth(_Settings.from_task(task_template, namespace))
