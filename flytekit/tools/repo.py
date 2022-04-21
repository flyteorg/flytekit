import tarfile
import tempfile
import typing

import click

from flytekit import FlyteContextManager, logger
from flytekit.clients.friendly import SynchronousFlyteClient
from flytekit.configuration import SerializationSettings
from flytekit.exceptions.user import FlyteEntityAlreadyExistsException
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models import task as task_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.models.core.identifier import Identifier, ResourceType
from flytekit.tools import fast_registration, module_loader
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
):
    """
    Package the given entities and the source code (if fast is enabled) into a package with the given name in output
    :param registrable_entities: Entities that can be serialized
    :param source: source folder
    :param output: output package name with suffix
    :param fast: fast enabled implies source code is bundled
    """
    if not registrable_entities:
        raise NoSerializableEntitiesError("Nothing to package")

    with tempfile.TemporaryDirectory() as output_tmpdir:
        persist_registrable_entities(registrable_entities, output_tmpdir)

        # If Fast serialization is enabled, then an archive is also created and packaged
        if fast:
            archive_fname = fast_registration.fast_package(source, output_tmpdir)
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
    options: typing.Optional[Options] = None,
):
    """
    Fist serialize and then package all entities
    """
    registrable_entities = serialize(pkgs, settings, source, options=options)
    package(registrable_entities, source, output, fast)


def register(
    registrable_entities: typing.List[RegistrableEntity],
    project: str,
    domain: str,
    version: str,
    client: SynchronousFlyteClient,
    source: str = ".",
    fast: bool = False,
):
    if fast:
        # TODO handle fast
        raise AssertionError("Fast not handled yet!")
    for entity, cp_entity in registrable_entities:
        try:
            if isinstance(cp_entity, task_models.TaskSpec):
                ident = Identifier(
                    resource_type=ResourceType.TASK, project=project, domain=domain, name=entity.name, version=version
                )
                client.create_task(task_identifer=ident, task_spec=cp_entity)
            elif isinstance(cp_entity, admin_workflow_models.WorkflowSpec):
                ident = Identifier(
                    resource_type=ResourceType.WORKFLOW,
                    project=project,
                    domain=domain,
                    name=entity.name,
                    version=version,
                )
                client.create_workflow(workflow_identifier=ident, workflow_spec=cp_entity)
            elif isinstance(cp_entity, launch_plan_models.LaunchPlanSpec):
                ident = Identifier(
                    resource_type=ResourceType.LAUNCH_PLAN,
                    project=project,
                    domain=domain,
                    name=entity.name,
                    version=version,
                )
                client.create_launch_plan(launch_plan_identifer=ident, launch_plan_spec=cp_entity)
            else:
                raise AssertionError(f"Unknown entity of type {type(cp_entity)}")
        except FlyteEntityAlreadyExistsException:
            logger.info(f"{entity.name} already exists")
        except Exception as e:
            logger.info(f"Failed to register entity {entity.name} with error {e}")
            raise e
