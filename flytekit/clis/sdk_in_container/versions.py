import typing

import rich_click as click
from click import Context, Parameter

from flytekit.clis.sdk_in_container.run import DynamicEntityLaunchCommand, RemoteEntityGroup, RunCommand, RunLevelParams
from flytekit.models.admin.common import Sort
from flytekit.models.common import NamedEntityIdentifier
from flytekit.remote import FlyteLaunchPlan, FlyteRemote, FlyteTask, FlyteWorkflow


class InstanceDisplayCommand(click.RichCommand):
    def __init__(self, name, h, **kwargs):
        super().__init__(name=name, help=h, **kwargs)


class DynamicEntityVersionCommand(click.RichGroup, DynamicEntityLaunchCommand):
    def __init__(self, name: str, h: str, entity_name: str, launcher: str, **kwargs):
        super(click.RichGroup, self).__init__(name, h, entity_name, launcher, **kwargs)

    def get_params(self, ctx: Context) -> typing.List[Parameter]:
        """
        returns empty list to avoid parent adding task/workflow/launchplan params
        """
        return []

    def list_commands(self, ctx: click.Context):
        run_params: RunLevelParams = ctx.obj
        named_entity = NamedEntityIdentifier(run_params.project, run_params.domain, ctx.info_name)
        _remote_instance: FlyteRemote = run_params.remote_instance()
        entity = self._fetch_entity(ctx)
        if isinstance(entity, FlyteTask):
            sorted_entities, _ = _remote_instance.client.list_tasks_paginated(
                named_entity, sort_by=Sort("created_at", Sort.Direction.DESCENDING)
            )
        elif isinstance(entity, FlyteLaunchPlan):
            sorted_entities, _ = _remote_instance.client.list_launch_plans_paginated(
                named_entity, sort_by=Sort("created_at", Sort.Direction.DESCENDING)
            )
        elif isinstance(entity, FlyteWorkflow):
            sorted_entities, _ = _remote_instance.client.list_workflows_paginated(
                named_entity, sort_by=Sort("created_at", Sort.Direction.DESCENDING)
            )
        else:
            raise ValueError(f"Unknown entity type {type(entity)}")

        self._entity_dict = {
            _entity.id.version: _entity.closure.created_at.strftime("%Y-%m-%d %H:%M:%S") for _entity in sorted_entities
        }
        return self._entity_dict.keys()

    def get_command(self, ctx, version):
        """
        returns version as command and created_at as help
        """
        if ctx.obj is None:
            ctx.obj = {}
        return InstanceDisplayCommand(name=version, h=f"Created At {self._entity_dict[version]}")


class RemoteEntityVersionGroup(RemoteEntityGroup):
    """
    click multicommand that retrieves launchplans from a remote flyte instance and display version of them.
    """

    def __init__(self, command_name: str):
        super().__init__(
            command_name,
        )

    def get_command(self, ctx: click.Context, name: str):
        if self._command_name in [self.LAUNCHPLAN_COMMAND, self.WORKFLOW_COMMAND]:
            return DynamicEntityVersionCommand(
                name=name,
                h=f"Display version of a {self._command_name}.",
                entity_name=name,
                launcher=DynamicEntityLaunchCommand.LP_LAUNCHER,
            )
        return DynamicEntityVersionCommand(
            name=name,
            h=f"Display version of a {self._command_name}.",
            entity_name=name,
            launcher=DynamicEntityLaunchCommand.TASK_LAUNCHER,
        )


class VersionCommand(RunCommand):
    _run_params: typing.Type[RunLevelParams] = RunLevelParams

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def list_commands(self, ctx: click.Context, add_remote: bool = True):
        self._files = sorted(self._files)
        if add_remote:
            self._files = self._files + [
                RemoteEntityGroup.LAUNCHPLAN_COMMAND,
                RemoteEntityGroup.WORKFLOW_COMMAND,
                RemoteEntityGroup.TASK_COMMAND,
            ]
        return self._files

    def get_command(self, ctx: click.Context, filename: str):
        super().get_command(ctx, filename)
        if filename == RemoteEntityGroup.LAUNCHPLAN_COMMAND:
            return RemoteEntityVersionGroup(RemoteEntityGroup.LAUNCHPLAN_COMMAND)
        elif filename == RemoteEntityGroup.WORKFLOW_COMMAND:
            return RemoteEntityVersionGroup(RemoteEntityGroup.WORKFLOW_COMMAND)
        elif filename == RemoteEntityGroup.TASK_COMMAND:
            return RemoteEntityVersionGroup(RemoteEntityGroup.TASK_COMMAND)
        else:
            raise NotImplementedError(f"File {filename} not found")


_run_help = """
Show the versions of the entity.
"""
version = VersionCommand(
    name="show-versions",
    help=_run_help,
)
