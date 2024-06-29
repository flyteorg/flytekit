import typing
from dataclasses import dataclass

import rich_click as click
from click import Context, Parameter

from flytekit.clis.sdk_in_container.run import DynamicEntityLaunchCommand, RemoteEntityGroup, RunBaseParams, RunCommand
from flytekit.models.admin.common import Sort
from flytekit.models.common import NamedEntityIdentifier
from flytekit.remote import FlyteLaunchPlan, FlyteRemote, FlyteTask


@dataclass
class VersionLevelParams(RunBaseParams):
    """
    This class is used to store the parameters for the version command.
    """

    pass


class InstanceDisplayCommand(click.RichCommand):
    """
    Dummy command that displays the version of the entity.
    """

    def __init__(self, name, help_msg, **kwargs):
        super().__init__(name=name, help=help_msg, **kwargs)


class DynamicEntityVersionCommand(click.RichGroup, DynamicEntityLaunchCommand):
    """
    Command that retrieves the versions of a remote entity.
    """

    def __init__(self, name: str, help_msg: str, entity_name: str, launcher: str, **kwargs):
        DynamicEntityLaunchCommand.__init__(self, name, help_msg, entity_name, launcher, **kwargs)

    def get_params(self, ctx: Context) -> typing.List[Parameter]:
        # we don't use super.get_params here, because DynamicEntityLaunchCommand.get_params adds the options of the entity
        return click.RichGroup.get_params(self, ctx)

    def list_commands(self, ctx: click.Context):
        run_params: VersionLevelParams = ctx.obj
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
        else:
            raise ValueError(f"Unknown entity type {type(entity)}")

        parse_creation_time = (
            lambda x: x.closure.created_at.strftime("%Y-%m-%d %H:%M:%S")
            if x.closure.created_at is not None
            else "Unknown Time"
        )
        self._entity_dict = {_entity.id.version: parse_creation_time(_entity) for _entity in sorted_entities}
        return self._entity_dict.keys()

    def get_command(self, ctx, version):
        if ctx.obj is None:
            ctx.obj = {}
        return InstanceDisplayCommand(name=version, help_msg=f"Created At {self._entity_dict[version]}")

    def invoke(self, ctx: Context) -> typing.Any:
        pass


class RemoteEntityVersionGroup(RemoteEntityGroup):
    """
    click multicommand that retrieves launchplans/tasks from a remote flyte instance and display version of them.
    """

    def __init__(self, command_name: str, help_msg: str):
        super().__init__(command_name, help_msg)

    def get_command(self, ctx: click.Context, name: str):
        if self._command_name in [self.LAUNCHPLAN_COMMAND, self.WORKFLOW_COMMAND]:
            return DynamicEntityVersionCommand(
                name=name,
                help_msg="",
                entity_name=name,
                launcher=DynamicEntityLaunchCommand.LP_LAUNCHER,
            )
        return DynamicEntityVersionCommand(
            name=name,
            help_msg="",
            entity_name=name,
            launcher=DynamicEntityLaunchCommand.TASK_LAUNCHER,
        )


class VersionCommand(RunCommand):
    _run_params: typing.Type[RunBaseParams] = VersionLevelParams

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._files = []

    def list_commands(self, ctx: click.Context):
        self._files = self._files + [
            RemoteEntityGroup.LAUNCHPLAN_COMMAND,
            RemoteEntityGroup.WORKFLOW_COMMAND,
            RemoteEntityGroup.TASK_COMMAND,
        ]
        return self._files

    def get_command(self, ctx: click.Context, filename: str):
        # call parent get_command to setup run_params
        super().get_command(ctx, filename)
        entity_version_help = f"Show the versions of the specified {filename}."
        if filename == RemoteEntityGroup.LAUNCHPLAN_COMMAND:
            return RemoteEntityVersionGroup(RemoteEntityGroup.LAUNCHPLAN_COMMAND, entity_version_help)
        elif filename == RemoteEntityGroup.WORKFLOW_COMMAND:
            return RemoteEntityVersionGroup(RemoteEntityGroup.WORKFLOW_COMMAND, entity_version_help)
        elif filename == RemoteEntityGroup.TASK_COMMAND:
            return RemoteEntityVersionGroup(RemoteEntityGroup.TASK_COMMAND, entity_version_help)
        else:
            raise NotImplementedError(f"File {filename} not found")


_version_help = """
Show the versions of the specified ``remote-task``, ``remote-launchplan``, or ``remote-workflow``.
Usage resembles the ``pyflyte run`` command, but instead of running the task, launchplan, or workflow,
it will display the versions of the remote entities and the time they were created.
"""
version = VersionCommand(
    name="show-versions",
    help=_version_help,
)
