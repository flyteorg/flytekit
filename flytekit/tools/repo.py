import os
import tarfile
import tempfile
import typing
from pathlib import Path

import click

from flytekit.configuration import FastSerializationSettings, ImageConfig, SerializationSettings
from flytekit.core.context_manager import FlyteContextManager
from flytekit.loggers import get_console, logger, rich_status
from flytekit.models import launch_plan
from flytekit.remote import FlyteRemote
from flytekit.remote.remote import RegistrationSkipped, _get_git_repo_url
from flytekit.tools import fast_registration, module_loader
from flytekit.tools.script_mode import _find_project_root
from flytekit.tools.serialize_helpers import get_registrable_entities, persist_registrable_entities
from flytekit.tools.translator import FlyteControlPlaneEntity, Options


class NoSerializableEntitiesError(Exception):
    pass


def serialize(
    pkgs: typing.List[str],
    settings: SerializationSettings,
    local_source_root: typing.Optional[str] = None,
    options: typing.Optional[Options] = None,
) -> typing.List[FlyteControlPlaneEntity]:
    """
    See :py:class:`flytekit.models.core.identifier.ResourceType` to match the trailing index in the file name with the
    entity type.
    :param options:
    :param settings: SerializationSettings to be used
    :param pkgs: Dot-delimited Python packages/subpackages to look into for serialization.
    :param local_source_root: Where to start looking for the code.
    """
    settings.source_root = local_source_root
    ctx = FlyteContextManager.current_context().with_serialization_settings(settings)
    with FlyteContextManager.with_context(ctx) as ctx:
        # Scan all modules. the act of loading populates the global singleton that contains all objects
        with module_loader.add_sys_path(local_source_root):
            with rich_status(f"Loading packages {pkgs} under source root {local_source_root}", "Loaded modules"):
                module_loader.just_load_modules(pkgs=pkgs)
        with rich_status("Serializing entities"):
            registrable_entities = get_registrable_entities(ctx, options=options)
        get_console().print(f"[green][✓][/] [bold white]Serialized [/] {len(registrable_entities)} flyte objects.")
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
    loaded_entities = serialize(pkgs, settings, local_source_root, options=options)
    persist_registrable_entities(loaded_entities, folder)


def package(
    serializable_entities: typing.List[FlyteControlPlaneEntity],
    source: str = ".",
    output: str = "./flyte-package.tgz",
    fast: bool = False,
    deref_symlinks: bool = False,
):
    """
    Package the given entities and the source code (if fast is enabled) into a package with the given name in output
    :param serializable_entities: Entities that can be serialized
    :param source: source folder
    :param output: output package name with suffix
    :param fast: fast enabled implies source code is bundled
    :param deref_symlinks: if enabled then symlinks are dereferenced during packaging
    """
    if not serializable_entities:
        raise NoSerializableEntitiesError("Nothing to package")

    with rich_status(f"Archiving {len(serializable_entities)} entities",
                     f"Packaged [bold white]{len(serializable_entities)}[/] flyte objects into {output}.") as status:
        with tempfile.TemporaryDirectory() as output_tmpdir:
            status.update(f"Persisting entities to {output_tmpdir}")
            persist_registrable_entities(serializable_entities, output_tmpdir)

            # If Fast serialization is enabled, then an archive is also created and packaged
            if fast:
                # If output exists and is a path within source, delete it so as to not re-bundle it again.
                if os.path.abspath(output).startswith(os.path.abspath(source)) and os.path.exists(output):
                    get_console().print(f"{output} already exists within {source}, deleting and re-creating it")
                    os.remove(output)
                archive_fname = fast_registration.fast_package(source, output_tmpdir, deref_symlinks)
                get_console().print(f"Fast mode enabled: compressed archive {archive_fname}")

            status.update(f"Archiving {output_tmpdir} to {output}")
            with tarfile.open(output, "w:gz") as tar:
                tar.add(output_tmpdir, arcname="")


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
    serializable_entities = serialize(pkgs, settings, source, options=options)
    package(serializable_entities, source, output, fast, deref_symlinks)


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
) -> typing.List[FlyteControlPlaneEntity]:
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
    ss.git_repo = _get_git_repo_url(project_root)
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
    fast: bool,
    package_or_module: typing.Tuple[str],
    remote: FlyteRemote,
    env: typing.Optional[typing.Dict[str, str]],
    dry_run: bool = False,
    activate_launchplans: bool = False,
):
    detected_root = find_common_root(package_or_module)
    logger.info(f"Detected root {detected_root}, using this to create deployable package...")
    fast_serialization_settings = None
    if fast:
        md5_bytes, native_url = remote.fast_package(detected_root, deref_symlinks, output)
        fast_serialization_settings = FastSerializationSettings(
            enabled=True,
            destination_dir=destination_dir,
            distribution_location=native_url,
        )

    # Create serialization settings
    serialization_settings = SerializationSettings(
        project=project,
        domain=domain,
        version=version,
        image_config=image_config,
        fast_serialization_settings=fast_serialization_settings,
        env=env,
    )

    if not version and fast:
        version = remote._version_from_hash(md5_bytes, serialization_settings, service_account, raw_data_prefix)  # noqa
        logger.info(f"Computed version is {version}")
    elif not version:
        raise ValueError("Version is required.")

    b = serialization_settings.new_builder()
    b.version = version
    serialization_settings = b.build()

    options = Options.default_from(k8s_service_account=service_account, raw_data_prefix=raw_data_prefix)

    # Load all the entities
    FlyteContextManager.push_context(remote.context)
    registrable_entities = load_packages_and_modules(
        serialization_settings, detected_root, list(package_or_module), options
    )
    FlyteContextManager.pop_context()
    if len(registrable_entities) == 0:
        raise NoSerializableEntitiesError("No entities were detected. Aborting!")

    for cp_entity in registrable_entities:
        is_lp = False
        if isinstance(cp_entity, launch_plan.LaunchPlan):
            og_id = cp_entity.id
            is_lp = True
        else:
            og_id = cp_entity.template.id
        user_friendly_name = f"{og_id.resource_type_name().lower()} {og_id.name}"
        try:
            if not dry_run:
                with rich_status(f"Registering {user_friendly_name}...") as status:
                    i = remote.raw_register(
                        cp_entity, serialization_settings, version=version, create_default_launchplan=False
                    )
                    if is_lp and activate_launchplans:
                        status.update(f"Activating {user_friendly_name}...")
                        remote.activate_launchplan(i)
                        get_console().print(
                            f"[green][✓][/] [bold white]Registered & Activated[/] {user_friendly_name}."
                        )
                    else:
                        get_console().print(
                            f"[green][✓][/] [bold white]Registered[/] {user_friendly_name} with version {i.version}."
                        )
            else:
                get_console().print(f"[yellow][-][/] Dry run mode, not registering {user_friendly_name}.")
        except RegistrationSkipped:
            get_console().print(f"[red][✗][/] Registration of {user_friendly_name} skipped.")
    click.secho(f"Successfully registered {len(registrable_entities)} entities", fg="green")
