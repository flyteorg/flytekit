import os
import tarfile
import tempfile
import typing
from pathlib import Path

import click
from flyteidl.admin.launch_plan_pb2 import LaunchPlan as _idl_admin_LaunchPlan
from flyteidl.admin.launch_plan_pb2 import LaunchPlanCreateRequest
from flyteidl.admin.task_pb2 import TaskCreateRequest
from flyteidl.admin.task_pb2 import TaskSpec as _idl_admin_TaskSpec
from flyteidl.admin.workflow_pb2 import WorkflowCreateRequest
from flyteidl.admin.workflow_pb2 import WorkflowSpec as _idl_admin_WorkflowSpec
from flyteidl.core import identifier_pb2

from flytekit.clients.friendly import SynchronousFlyteClient
from flytekit.clis.helpers import hydrate_registration_parameters
from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import FlyteContextManager
from flytekit.exceptions.user import FlyteEntityAlreadyExistsException
from flytekit.loggers import logger
from flytekit.tools import fast_registration, module_loader
from flytekit.tools.script_mode import _find_project_root
from flytekit.tools.serialize_helpers import RegistrableEntity, get_registrable_entities, persist_registrable_entities
from flytekit.tools.translator import Options


class NoSerializableEntitiesError(Exception):
    pass


def serialize(
    pkgs: typing.List[str],
    settings: SerializationSettings,
    local_source_root: typing.Optional[str] = None,
    options: typing.Optional[Options] = None,
) -> typing.List[RegistrableEntity]:
    """
    See :py:class:`flytekit.models.core.identifier.ResourceType` to match the trailing index in the file name with the
    entity type.
    :param options:
    :param settings: SerializationSettings to be used
    :param pkgs: Dot-delimited Python packages/subpackages to look into for serialization.
    :param local_source_root: Where to start looking for the code.
    """

    ctx = FlyteContextManager.current_context().with_serialization_settings(settings)
    with FlyteContextManager.with_context(ctx) as ctx:
        # Scan all modules. the act of loading populates the global singleton that contains all objects
        with module_loader.add_sys_path(local_source_root):
            click.secho(f"Loading packages {pkgs} under source root {local_source_root}", fg="yellow")
            module_loader.just_load_modules(pkgs=pkgs)

        registrable_entities = get_registrable_entities(ctx, options=options)
        click.secho(f"Successfully serialized {len(registrable_entities)} flyte objects", fg="green")
        return registrable_entities


def serialize_to_folder(
    pkgs: typing.List[str],
    settings: SerializationSettings,
    local_source_root: typing.Optional[str] = None,
    folder: str = ".",
    options: typing.Optional[Options] = None,
):
    """
    Serialize the given set of python packages to a folder
    """
    loaded_entities = serialize(pkgs, settings, local_source_root, options=options)
    persist_registrable_entities(loaded_entities, folder)


def package(
    registrable_entities: typing.List[RegistrableEntity],
    source: str = ".",
    output: str = "./flyte-package.tgz",
    fast: bool = False,
    deref_symlinks: bool = False,
):
    """
    Package the given entities and the source code (if fast is enabled) into a package with the given name in output
    :param registrable_entities: Entities that can be serialized
    :param source: source folder
    :param output: output package name with suffix
    :param fast: fast enabled implies source code is bundled
    :param deref_symlinks: if enabled then symlinks are dereferenced during packaging
    """
    if not registrable_entities:
        raise NoSerializableEntitiesError("Nothing to package")

    with tempfile.TemporaryDirectory() as output_tmpdir:
        persist_registrable_entities(registrable_entities, output_tmpdir)

        # If Fast serialization is enabled, then an archive is also created and packaged
        if fast:
            # If output exists and is a path within source, delete it so as to not re-bundle it again.
            if os.path.abspath(output).startswith(os.path.abspath(source)) and os.path.exists(output):
                click.secho(f"{output} already exists within {source}, deleting and re-creating it", fg="yellow")
                os.remove(output)
            archive_fname = fast_registration.fast_package(source, output_tmpdir, deref_symlinks)
            click.secho(f"Fast mode enabled: compressed archive {archive_fname}", dim=True)

        with tarfile.open(output, "w:gz") as tar:
            tar.add(output_tmpdir, arcname="")

    click.secho(f"Successfully packaged {len(registrable_entities)} flyte objects into {output}", fg="green")


def serialize_and_package(
    pkgs: typing.List[str],
    settings: SerializationSettings,
    source: str = ".",
    output: str = "./flyte-package.tgz",
    fast: bool = False,
    deref_symlinks: bool = False,
    options: typing.Optional[Options] = None,
):
    """
    Fist serialize and then package all entities
    """
    registrable_entities = serialize(pkgs, settings, source, options=options)
    package(registrable_entities, source, output, fast, deref_symlinks)


def register(
    registrable_entities: typing.List[RegistrableEntity],
    project: str,
    domain: str,
    version: str,
    client: SynchronousFlyteClient,
):
    # The incoming registrable entities are already in base protobuf form, not model form, so we use the
    # raw client's methods instead of the friendly client's methods by calling super
    for admin_entity in registrable_entities:
        try:
            if isinstance(admin_entity, _idl_admin_TaskSpec):
                ident, task_spec = hydrate_registration_parameters(
                    identifier_pb2.TASK, project, domain, version, admin_entity
                )
                logger.debug(f"Creating task {ident}")
                super(SynchronousFlyteClient, client).create_task(TaskCreateRequest(id=ident, spec=task_spec))
            elif isinstance(admin_entity, _idl_admin_WorkflowSpec):
                ident, wf_spec = hydrate_registration_parameters(
                    identifier_pb2.WORKFLOW, project, domain, version, admin_entity
                )
                logger.debug(f"Creating workflow {ident}")
                super(SynchronousFlyteClient, client).create_workflow(WorkflowCreateRequest(id=ident, spec=wf_spec))
            elif isinstance(admin_entity, _idl_admin_LaunchPlan):
                ident, admin_lp = hydrate_registration_parameters(
                    identifier_pb2.LAUNCH_PLAN, project, domain, version, admin_entity
                )
                logger.debug(f"Creating launch plan {ident}")
                super(SynchronousFlyteClient, client).create_launch_plan(
                    LaunchPlanCreateRequest(id=ident, spec=admin_lp.spec)
                )
            else:
                raise AssertionError(f"Unknown entity of type {type(admin_entity)}")
        except FlyteEntityAlreadyExistsException:
            logger.info(f"{admin_entity} already exists")
        except Exception as e:
            logger.info(f"Failed to register entity {admin_entity} with error {e}")
            raise e


def find_common_root(
    pkgs_or_mods: typing.Union[typing.Tuple[str], typing.List[str]],
) -> Path:
    """
    Given an arbitrary list of folders and files, this function will use the script mode function to walk up
    the filesystem to find the first folder without an init file. If all the folders and files resolve to
    the same root folder, then that Path is returned. Otherwise an error is raised.

    :param pkgs_or_mods:
    :return: The common detected root path, the output of _find_project_root
    """
    project_root = None
    for pm in pkgs_or_mods:
        root = _find_project_root(pm)
        if project_root is None:
            project_root = root
        else:
            if project_root != root:
                raise ValueError(f"Specified module {pm} has root {root} but {project_root} already specified")

    logger.debug(f"Common root folder detected as {str(project_root)}")

    return project_root


def load_packages_and_modules(
    ss: SerializationSettings,
    project_root: Path,
    pkgs_or_mods: typing.List[str],
    options: typing.Optional[Options] = None,
) -> typing.List[RegistrableEntity]:
    """
    The project root is added as the first entry to sys.path, and then all the specified packages and modules
    given are loaded with all submodules. The reason for prepending the entry is to ensure that the name that
    the various modules are loaded under are the fully-resolved name.

    For example, using flytesnacks cookbook, if you are in core/ and you call this function with
    ``flyte_basics/hello_world.py control_flow/``, the ``hello_world`` module would be loaded
    as ``core.flyte_basics.hello_world`` even though you're already in the core/ folder.

    :param ss:
    :param project_root:
    :param pkgs_or_mods:
    :param options:
    :return: The common detected root path, the output of _find_project_root
    """

    pkgs_and_modules = []
    for pm in pkgs_or_mods:
        p = Path(pm).resolve()
        rel_path_from_root = p.relative_to(project_root)
        # One day we should learn how to do this right. This is not the right way to load a python module
        # from a file. See pydoc.importfile for inspiration
        dot_delineated = os.path.splitext(rel_path_from_root)[0].replace(os.path.sep, ".")  # noqa

        logger.debug(
            f"User specified arg {pm} has {str(rel_path_from_root)} relative path loading it as {dot_delineated}"
        )
        pkgs_and_modules.append(dot_delineated)

    registrable_entities = serialize(pkgs_and_modules, ss, str(project_root), options)

    return registrable_entities
