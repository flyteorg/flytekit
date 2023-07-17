import os
import pathlib
import typing

import rich_click as click
from typing_extensions import OrderedDict

from flytekit.clis.sdk_in_container.constants import CTX_MODULE, CTX_PROJECT_ROOT
from flytekit.clis.sdk_in_container.run import RUN_LEVEL_PARAMS_KEY, get_entities_in_file, load_naive_entity
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.workflow import PythonFunctionWorkflow
from flytekit.tools.script_mode import _find_project_root
from flytekit.tools.translator import get_serializable


def get_workflow_command_base_params() -> typing.List[click.Option]:
    """
    Return the set of base parameters added to every pyflyte build workflow subcommand.
    """
    return [
        click.Option(
            param_decls=["--fast"],
            required=False,
            is_flag=True,
            default=False,
            help="Use fast serialization. The image won't contain the source code. The value is false by default.",
        ),
    ]


def build_command(ctx: click.Context, entity: typing.Union[PythonFunctionWorkflow, PythonTask]):
    """
    Returns a function that is used to implement WorkflowCommand and build an image for flyte workflows.
    """

    def _build(*args, **kwargs):
        m = OrderedDict()
        options = None
        run_level_params = ctx.obj[RUN_LEVEL_PARAMS_KEY]

        project, domain = run_level_params.get("project"), run_level_params.get("domain")
        serialization_settings = SerializationSettings(
            project=project,
            domain=domain,
            image_config=ImageConfig.auto_default_image(),
        )
        if not run_level_params.get("fast"):
            serialization_settings.source_root = ctx.obj[RUN_LEVEL_PARAMS_KEY].get(CTX_PROJECT_ROOT)

        _ = get_serializable(m, settings=serialization_settings, entity=entity, options=options)

    return _build


class WorkflowCommand(click.MultiCommand):
    """
    click multicommand at the python file layer, subcommands should be all the workflows in the file.
    """

    def __init__(self, filename: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = pathlib.Path(filename).resolve()

    def list_commands(self, ctx):
        entities = get_entities_in_file(self._filename.__str__(), False)
        return entities.all()

    def get_command(self, ctx, exe_entity):
        """
        This command uses the filename with which this command was created, and the string name of the entity passed
          after the Python filename on the command line, to load the Python object, and then return the Command that
          click should run.
        :param ctx: The click Context object.
        :param exe_entity: string of the flyte entity provided by the user. Should be the name of a workflow, or task
          function.
        :return:
        """
        rel_path = os.path.relpath(self._filename)
        if rel_path.startswith(".."):
            raise ValueError(
                f"You must call pyflyte from the same or parent dir, {self._filename} not under {os.getcwd()}"
            )

        project_root = _find_project_root(self._filename)
        rel_path = self._filename.relative_to(project_root)
        module = os.path.splitext(rel_path)[0].replace(os.path.sep, ".")

        ctx.obj[RUN_LEVEL_PARAMS_KEY][CTX_PROJECT_ROOT] = project_root
        ctx.obj[RUN_LEVEL_PARAMS_KEY][CTX_MODULE] = module

        entity = load_naive_entity(module, exe_entity, project_root)

        cmd = click.Command(
            name=exe_entity,
            callback=build_command(ctx, entity),
            help=f"Build an image for {module}.{exe_entity}.",
        )
        return cmd


class BuildCommand(click.MultiCommand):
    """
    A click command group for building a image for flyte workflows & tasks in a file.
    """

    def __init__(self, *args, **kwargs):
        params = get_workflow_command_base_params()
        super().__init__(*args, params=params, **kwargs)

    def list_commands(self, ctx):
        return [str(p) for p in pathlib.Path(".").glob("*.py") if str(p) != "__init__.py"]

    def get_command(self, ctx, filename):
        if ctx.obj:
            ctx.obj[RUN_LEVEL_PARAMS_KEY] = ctx.params
        return WorkflowCommand(filename, name=filename, help="Build an image for [workflow|task]")


_build_help = """
This command can build an image for a workflow or a task from the command line, for fully self-contained scripts.
"""

build = BuildCommand(
    name="build",
    help=_build_help,
)
