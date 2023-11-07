import typing
from dataclasses import dataclass, field

import rich_click as click
from typing_extensions import OrderedDict

from flytekit.clis.sdk_in_container.run import RunCommand, RunLevelParams, WorkflowCommand
from flytekit.clis.sdk_in_container.utils import make_click_option_field
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.workflow import PythonFunctionWorkflow
from flytekit.tools.translator import get_serializable


@dataclass
class BuildParams(RunLevelParams):

    destination_dir: str = field(default="")
    service_account: str = field(default="")
    raw_output_data_prefix: str = field(default="")
    dump_snippet: bool = field(default=False)
    wait_execution: bool = field(default=False)
    remote: bool = field(default=False)
    limit: bool = field(default=False)
    cluster_pool: str = field(default="")
    max_parallelism: int = field(default=0)
    overwrite_cache: bool = field(default=False)
    name: str = field(default="")
    annotations: typing.Dict[str, str] = field(default_factory=dict)
    labels: typing.Dict[str, str] = field(default_factory=dict)
    disable_notifications: bool = field(default=False)
    envvars: typing.Dict[str, str] = field(default_factory=dict)
    tags: typing.List[str] = field(default_factory=list)

    fast: bool = make_click_option_field(
        click.Option(
            param_decls=["--fast"],
            required=False,
            is_flag=True,
            default=False,
            show_default=True,
            help="Use fast serialization. The image won't contain the source code. The value is false by default.",
        )
    )


def build_command(ctx: click.Context, entity: typing.Union[PythonFunctionWorkflow, PythonTask]):
    """
    Returns a function that is used to implement WorkflowCommand and build an image for flyte workflows.
    """

    def _build(*args, **kwargs):
        m = OrderedDict()
        options = None
        build_params: BuildParams = ctx.obj

        serialization_settings = SerializationSettings(
            project=build_params.project,
            domain=build_params.domain,
            image_config=ImageConfig.auto_default_image(),
        )
        if not build_params.fast:
            serialization_settings.source_root = build_params.computed_params.project_root

        _ = get_serializable(m, settings=serialization_settings, entity=entity, options=options)

    return _build


class BuildWorkflowCommand(WorkflowCommand):
    """
    click multicommand at the python file layer, subcommands should be all the workflows in the file.
    """

    def _create_command(
        self,
        ctx: click.Context,
        entity_name: str,
        run_level_params: RunLevelParams,
        loaded_entity: typing.Any,
        is_workflow: bool,
    ):
        cmd = click.Command(
            name=entity_name,
            callback=build_command(ctx, loaded_entity),
            help=f"Build an image for {run_level_params.computed_params.module}.{entity_name}.",
        )
        return cmd


class BuildCommand(RunCommand):
    """
    A click command group for building a image for flyte workflows & tasks in a file.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, params=BuildParams.options(), **kwargs)

    def list_commands(self, ctx, *args, **kwargs):
        return super().list_commands(ctx, add_remote=False)

    def _get_context_obj(self, params: typing.Dict) -> RunLevelParams:
        return BuildParams.from_dict(params)

    def get_command(self, ctx, filename):
        super().get_command(ctx, filename)
        return BuildWorkflowCommand(filename, name=filename, help=f"Build an image for [workflow|task] from {filename}")


_build_help = """
This command can build an image for a workflow or a task from the command line, for fully self-contained scripts.
"""

build = BuildCommand(
    name="build",
    help=_build_help,
)
