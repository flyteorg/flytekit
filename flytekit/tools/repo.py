import asyncio
import functools
import os
import tarfile
import tempfile
import typing
from pathlib import Path

import click
from rich import print as rprint

from flytekit.configuration import FastSerializationSettings, ImageConfig, SerializationSettings
from flytekit.constants import CopyFileDetection
from flytekit.core.context_manager import FlyteContextManager
from flytekit.loggers import logger
from flytekit.models import launch_plan, task
from flytekit.models.core.identifier import Identifier
from flytekit.remote import FlyteRemote
from flytekit.remote.remote import RegistrationSkipped, _get_git_repo_url
from flytekit.tools import fast_registration, module_loader
from flytekit.tools.script_mode import _find_project_root
from flytekit.tools.serialize_helpers import get_registrable_entities, persist_registrable_entities
from flytekit.tools.translator import FlyteControlPlaneEntity, Options


class NoSerializableEntitiesError(Exception):
    pass


def serialize_load_only(
    pkgs: typing.List[str],
    settings: SerializationSettings,
    local_source_root: typing.Optional[str] = None,
):
    """
    See :py:class:`flytekit.models.core.identifier.ResourceType` to match the trailing index in the file name with the
    entity type.
    :param settings: SerializationSettings to be used
    :param pkgs: Dot-delimited Python packages/subpackages to look into for serialization.
    :param local_source_root: Where to start looking for the code.
    """
    settings.source_root = local_source_root
    ctx_builder = FlyteContextManager.current_context().with_serialization_settings(settings)
    with FlyteContextManager.with_context(ctx_builder):
        # Scan all modules. the act of loading populates the global singleton that contains all objects
        with module_loader.add_sys_path(local_source_root):
            click.secho(f"Loading packages {pkgs} under source root {local_source_root}", fg="yellow")
            module_loader.just_load_modules(pkgs=pkgs)


def serialize_get_control_plane_entities(
    settings: SerializationSettings,
    local_source_root: typing.Optional[str] = None,
    options: typing.Optional[Options] = None,
    is_registration: bool = False,
) -> typing.List[FlyteControlPlaneEntity]:
    """
    See :py:class:`flytekit.models.core.identifier.ResourceType` to match the trailing index in the file name with the
    entity type.
    :param options:
    :param settings: SerializationSettings to be used
    :param pkgs: Dot-delimited Python packages/subpackages to look into for serialization.
    :param local_source_root: Where to start looking for the code.
    :param is_registration: Whether this is happening for registration.
    """
    settings.source_root = local_source_root
    ctx_builder = FlyteContextManager.current_context().with_serialization_settings(settings)
    with FlyteContextManager.with_context(ctx_builder) as ctx:
        registrable_entities = get_registrable_entities(ctx, options=options)
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
    if folder is None:
        folder = "."
    serialize_load_only(pkgs, settings, local_source_root)
    loaded_entities = serialize_get_control_plane_entities(settings, local_source_root, options=options)
    click.secho(
        f"Successfully serialized {len(loaded_entities)} flyte entities",
        fg="green",
    )
    persist_registrable_entities(loaded_entities, folder)


def package(
    serializable_entities: typing.List[FlyteControlPlaneEntity],
    source: str = ".",
    output: str = "./flyte-package.tgz",
    deref_symlinks: bool = False,
    fast_options: typing.Optional[fast_registration.FastPackageOptions] = None,
):
    """
    Package the given entities and the source code (if fast is enabled) into a package with the given name in output
    :param serializable_entities: Entities that can be serialized
    :param source: source folder
    :param output: output package name with suffix
    :param deref_symlinks: if enabled then symlinks are dereferenced during packaging
    :param fast_options:

    Temporarily, for fast register, specify both the fast arg as well as copy_style fast == True with
    copy_style == None means use the old fast register tar'ring method.
    In the future the fast bool will be removed, and copy_style == None will mean do not fast register.
    """
    if not serializable_entities:
        raise NoSerializableEntitiesError("Nothing to package")

    with tempfile.TemporaryDirectory() as output_tmpdir:
        persist_registrable_entities(serializable_entities, output_tmpdir)

        # If Fast serialization is enabled, then an archive is also created and packaged
        if fast_options and fast_options.copy_style != CopyFileDetection.NO_COPY:
            # If output exists and is a path within source, delete it so as to not re-bundle it again.
            if os.path.abspath(output).startswith(os.path.abspath(source)) and os.path.exists(output):
                click.secho(f"{output} already exists within {source}, deleting and re-creating it", fg="yellow")
                os.remove(output)
            archive_fname = fast_registration.fast_package(source, output_tmpdir, deref_symlinks, options=fast_options)
            click.secho(f"Fast mode enabled: compressed archive {archive_fname}", dim=True)

        with tarfile.open(output, "w:gz") as tar:
            files: typing.List[str] = os.listdir(output_tmpdir)
            for ws_file in files:
                tar.add(os.path.join(output_tmpdir, ws_file), arcname=ws_file)

    click.secho(f"Successfully packaged {len(serializable_entities)} flyte objects into {output}", fg="green")


def serialize_and_package(
    pkgs: typing.List[str],
    settings: SerializationSettings,
    source: str = ".",
    output: str = "./flyte-package.tgz",
    deref_symlinks: bool = False,
    options: typing.Optional[Options] = None,
    fast_options: typing.Optional[fast_registration.FastPackageOptions] = None,
):
    """
    Fist serialize and then package all entities
    Temporarily for fast package, specify both the fast arg as well as copy_style.
    fast == True with copy_style == None means use the old fast register tar'ring method.
    """
    serialize_load_only(pkgs, settings, source)
    serializable_entities = serialize_get_control_plane_entities(settings, source, options=options)
    click.secho(
        f"Successfully serialized {len(serializable_entities)} flyte entities",
        fg="green",
    )
    package(serializable_entities, source, output, deref_symlinks, fast_options)


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


def list_packages_and_modules(
    project_root: Path,
    pkgs_or_mods: typing.List[str],
) -> typing.List[str]:
    """
    This is a helper function that returns the input list of python packages/modules as a dot delinated list
    relative to the given project_root.

    :param project_root:
    :param pkgs_or_mods:
    :return: List of packages/modules, dot delineated.
    """
    pkgs_and_modules: typing.List[str] = []
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

    return pkgs_and_modules


def print_registration_status(
    i: Identifier,
    success: bool = True,
    activation: bool = False,
    dry_run: bool = False,
    console_url: str = None,
    verbosity: int = 0,
) -> None:
    color = "green" if success else "red"
    state_ind = "\r[✔]" if success else "\r[✘]"
    activation_note = " (Activated)" if activation else ""

    name_words = i.resource_type_name().replace("_", " ").split()
    name = " ".join(word.capitalize() for word in name_words)

    if success and not dry_run:
        if verbosity > 0:
            rprint(f"[{color}]{state_ind} {name}: [cyan underline]{console_url}[/]" + activation_note)
        else:
            rprint(
                f"[{color}]{state_ind} {name}: [cyan underline][link={console_url}]{i.name}[/link][/]" + activation_note
            )
    elif success and dry_run:
        rprint(f"[{color}]{state_ind} {name}: {i.name} (Dry run Mode)")
    elif not success:
        rprint(f"[{color}]{state_ind} {name}: {i.name} (Failed)")


def register(
    project: str,
    domain: str,
    image_config: ImageConfig,
    output: str,
    destination_dir: str,
    service_account: str,
    raw_data_prefix: str,
    version: typing.Optional[str],
    deref_symlinks: bool,
    package_or_module: typing.Tuple[str],
    remote: FlyteRemote,
    copy_style: CopyFileDetection,
    env: typing.Optional[typing.Dict[str, str]],
    dry_run: bool = False,
    activate_launchplans: bool = False,
    skip_errors: bool = False,
    show_files: bool = False,
    verbosity: int = 0,
):
    """
    Temporarily, for fast register, specify both the fast arg as well as copy_style.
    fast == True with copy_style == None means use the old fast register tar'ring method.
    """
    detected_root = find_common_root(package_or_module)
    click.secho(f"Detected Root {detected_root}, using this to create deployable package...", fg="yellow")

    # Create serialization settings
    # Todo: Rely on default Python interpreter for now, this will break custom Spark containers
    serialization_settings = SerializationSettings(
        project=project,
        domain=domain,
        version=version,
        image_config=image_config,
        fast_serialization_settings=None,  # should probably add incomplete fast settings
        env=env,
    )

    if not version and copy_style == CopyFileDetection.NO_COPY:
        click.secho("Version is required.", fg="red")
        return

    b = serialization_settings.new_builder()
    serialization_settings = b.build()

    options = Options.default_from(k8s_service_account=service_account, raw_data_prefix=raw_data_prefix)

    # Load all the entities
    FlyteContextManager.push_context(remote.context)
    serialization_settings.git_repo = _get_git_repo_url(str(detected_root))
    pkgs_and_modules = list_packages_and_modules(detected_root, list(package_or_module))

    # NB: The change here is that the loading of user code _cannot_ depend on fast register information (the computed
    #     version, upload native url, hash digest, etc.).
    serialize_load_only(pkgs_and_modules, serialization_settings, str(detected_root))

    # Fast registration is handled after module loading
    if copy_style != CopyFileDetection.NO_COPY:
        md5_bytes, native_url = remote.fast_package(
            detected_root,
            deref_symlinks,
            output,
            options=fast_registration.FastPackageOptions([], copy_style=copy_style, show_files=show_files),
        )
        # update serialization settings from fast register output
        fast_serialization_settings = FastSerializationSettings(
            enabled=True,
            destination_dir=destination_dir,
            distribution_location=native_url,
        )
        serialization_settings.fast_serialization_settings = fast_serialization_settings
        if not version:
            version = remote._version_from_hash(md5_bytes, serialization_settings, service_account, raw_data_prefix)  # noqa
            serialization_settings.version = version
            click.secho(f"Computed version is {version}", fg="yellow")

    registrable_entities = serialize_get_control_plane_entities(
        serialization_settings, str(detected_root), options, is_registration=True
    )
    click.secho(
        f"Serializing and registering {len(registrable_entities)} flyte entities",
        fg="green",
    )

    FlyteContextManager.pop_context()
    if len(registrable_entities) == 0:
        click.secho("No Flyte entities were detected. Aborting!", fg="red")
        return

    def _raw_register(cp_entity: FlyteControlPlaneEntity):
        is_lp = False
        if isinstance(cp_entity, launch_plan.LaunchPlan):
            og_id = cp_entity.id
            is_lp = True
        else:
            og_id = cp_entity.template.id
        try:
            if not dry_run:
                try:
                    i = remote.raw_register(
                        cp_entity, serialization_settings, version=version, create_default_launchplan=False
                    )
                    console_url = remote.generate_console_url(i)
                    print_activation_message = False
                    if is_lp:
                        if activate_launchplans:
                            remote.activate_launchplan(i)
                            print_activation_message = True
                        if cp_entity.should_auto_activate:
                            print_activation_message = True
                    print_registration_status(
                        i, console_url=console_url, verbosity=verbosity, activation=print_activation_message
                    )

                except Exception as e:
                    if not skip_errors:
                        raise e
                    print_registration_status(og_id, success=False)
            else:
                print_registration_status(og_id, dry_run=True)
        except RegistrationSkipped:
            print_registration_status(og_id, success=False)

    async def _register(entities: typing.List[task.TaskSpec]):
        loop = asyncio.get_running_loop()
        tasks = []
        for entity in entities:
            tasks.append(loop.run_in_executor(None, functools.partial(_raw_register, entity)))
        await asyncio.gather(*tasks)
        return

    # concurrent register
    cp_task_entities = list(filter(lambda x: isinstance(x, task.TaskSpec), registrable_entities))
    asyncio.run(_register(cp_task_entities))
    # serial register
    cp_other_entities = list(filter(lambda x: not isinstance(x, task.TaskSpec), registrable_entities))
    for entity in cp_other_entities:
        _raw_register(entity)

    click.secho(f"Successfully registered {len(registrable_entities)} entities", fg="green")
