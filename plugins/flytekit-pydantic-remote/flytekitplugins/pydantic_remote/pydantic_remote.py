from collections import defaultdict
from typing import Optional

from flytekitplugins.pydantic_remote.pydantic_workflow_interface import (
    PydanticWorkflowInterface,
    create_pydantic_workflow_interface,
)

from flytekit.exceptions import user as user_exceptions
from flytekit.models import filters as flyte_filters
from flytekit.models.admin import common as admin_common_models
from flytekit.models.admin.workflow import Workflow
from flytekit.models.common import NamedEntityIdentifier
from flytekit.remote import FlyteRemote


class PydanticFlyteRemote(FlyteRemote):
    def _list_all_workflows(
        self,
        project: Optional[str] = None,
        domain: Optional[str] = None,
        filters: Optional[list[flyte_filters.Filter]] = None,
        sort_by: Optional[admin_common_models.Sort] = None,
        max_workflows_to_fetch: Optional[int] = None,
    ) -> list[Workflow]:
        """Return all workflows give project, domain, and filters."""
        project = project or self.default_project
        domain = domain or self.default_domain
        sort_by = sort_by or admin_common_models.Sort(
            "created_at", admin_common_models.Sort.Direction.DESCENDING
        )
        all_workflows = []
        token = None
        while token != "":
            wfs, token = self.client.list_workflows_paginated(
                identifier=NamedEntityIdentifier(project, domain),
                token=token,
                filters=filters,
                sort_by=sort_by,
            )
            all_workflows.extend(wfs)
            if max_workflows_to_fetch and len(all_workflows) >= max_workflows_to_fetch:
                break
        return all_workflows

    def fetch_pydantic_workflow(
        self,
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
    ) -> PydanticWorkflowInterface:
        """Fetch PydanticWorkflowInterface from FlyteRemote.

        Args:
            project: Name of Flyte project. If None, uses the default_project attribute.
            domain: Domain of Flyte project. If None, uses the default_project attribute.
            name: Name of Flyte workflow.
            version: Version of Flyte workflow. If None, gets the latest version of the entity.

        Returns:
            Instantiated PydanticWorkflowInterface
        """
        if name is None:
            raise user_exceptions.FlyteAssertion(
                "the 'name' argument must be specified."
            )
        model = create_pydantic_workflow_interface(
            project or self.default_project,
            domain or self.default_domain,
            name,
            version,
            remote=self,
        )
        return model

    def fetch_pydantic_workflows(
        self,
        project: Optional[str] = None,
        domain: Optional[str] = None,
        filters: Optional[list[flyte_filters.Filter]] = None,
        sort_by: Optional[admin_common_models.Sort] = None,
        max_workflows_to_fetch: Optional[int] = 200,
    ) -> dict[str, PydanticWorkflowInterface]:
        """Get registered workflows from a given Flyte project and domain.

        Args:
            project: Name of Flyte project. If None, uses the default_project attribute.
            domain: Domain of Flyte project. If None, uses the default_project attribute.
            filters: Optional filters to use when fetching. Defaults to None.
            sort_by: Optional alternative sorting. Defaults to sorting by most recent.
            max_workflows_to_fetch: Maximum number of workflows to fetch from remote.
                Defaults to 200, but can be increased if you need to fetch and old version,
                or set to `None` to fetch all workflows.

        Returns:
            Dict with key=name, value=PydanticWorkflowInterface objects
        """
        fetched_workflows = self._list_all_workflows(
            project,
            domain,
            filters=filters or [],
            sort_by=sort_by,
            max_workflows_to_fetch=max_workflows_to_fetch,
        )
        workflow_versions = defaultdict(list)
        for wf in fetched_workflows:
            workflow_versions[wf.id.name].append(wf)
        pydantic_models = {}
        for wf_versions in workflow_versions.values():
            first, *others = wf_versions
            other_versions = [x.id.version for x in others]
            model = create_pydantic_workflow_interface(
                first.id.project,
                first.id.domain,
                first.id.name,
                first.id.version,
                remote=self,
                other_versions=other_versions,
            )
            pydantic_models[first.id.name] = model
        return pydantic_models
