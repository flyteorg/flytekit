import os
import tarfile
import tempfile
from typing import List

import click
from flyteidl.core import identifier_pb2
from google.protobuf.pyext.cpp_message import GeneratedProtocolMessageType

from flytekit import configuration
from flytekit.clients import friendly
from flytekit.clis.flyte_cli.main import _extract_and_register
from flytekit.configuration import FastSerializationSettings, ImageConfig, SerializationSettings
from flytekit.core import context_manager
from flytekit.exceptions.user import FlyteValidationException
from flytekit.tools import fast_registration, module_loader, serialize_helpers


@click.command("run")
@click.argument(
    "workflow_path_and_name",
)
@click.option(
    "image",
    "-i",
    "--image",
    required=False,
    type=str,
    # TODO: fix default images push gh workflow
    # default="ghcr.io/flyteorg/flytekit:py39-latest",
    default="pyflyte-fast-execute:latest",
    help="Image used to register and run.",
)
@click.option(
    "help",
    "-h",
    "--help",
    required=False,
    is_flag=True,
    help="Shows inputs to workflow and potentially the workflow docstring",
)
@click.pass_context
def run(
    ctx,
    workflow_path_and_name,
    image,
    help=None,
):
    """
    TODO description (remember to document parameters)
    """
    # TODO is it `--help`? Figure out a way to parse the inputs to the workflow
    #

    split_input = workflow_path_and_name.split(":")
    if len(split_input) != 2:
        raise FlyteValidationException(f"Input {workflow_path_and_name} must be in format '<module>:<worfklow>'")

    workflow_path, workflow_name = split_input
    # TODO: find the project root and use it to build the package name
    pkgs = ["flyte.workflows"]
    source = "/home/eduardo/tmp/pyflyte-run/"
    destination_dir = "/root"

    # Load the code
    #

    image_config = ImageConfig.validate_single_image(image)
    serialization_settings = SerializationSettings(
        image_config=image_config,
        fast_serialization_settings=FastSerializationSettings(
            enabled=True,
            destination_dir=destination_dir,
        ),
        # TODO: do I really need to define a python interpreter?
        # python_interpreter=python_interpreter,
    )
    ctx = context_manager.FlyteContextManager.current_context().with_serialization_settings(serialization_settings)
    # TODO: figure out source
    with context_manager.FlyteContextManager.with_context(ctx) as ctx:
        # Scan all modules. the act of loading populates the global singleton that contains all objects
        # TODO: find root of project
        with module_loader.add_sys_path(source):
            click.secho(f"Loading code {workflow_path}", fg="yellow")
            module_loader.just_load_modules(pkgs=pkgs)

    registrable_entities = serialize_helpers.get_registrable_entities(ctx)

    if registrable_entities:
        # Open a temp directory and dump the contents of the digest.
        tmp_dir = tempfile.TemporaryDirectory()

        # Write
        source_path = os.fspath(source)
        digest = fast_registration.compute_digest(source_path)
        archive_fname = os.path.join(tmp_dir.name, f"{digest}.tar.gz")
        # Write using gzip
        with tarfile.open(archive_fname, "w:gz") as tar:
            tar.add(source, arcname="", filter=fast_registration.filter_tar_file_fn)

        serialize_helpers.persist_registrable_entities(registrable_entities, tmp_dir.name)

        # List al pb files
        pb_files = []
        for f in os.listdir(tmp_dir.name):
            if f.endswith(".pb"):
                pb_files.append(os.path.join(tmp_dir.name, f))

        version = str(os.getpid())  # TODO: find a better source of entropy

        # TODO Get signed url from flyteadmin
        # TODO: Patch task specs with the signed url before writing them to disk
        full_remote_path = f"s3://my-s3-bucket/fast-register-test/{digest}.tar.gz"
        flyte_ctx = context_manager.FlyteContextManager.current_context()
        flyte_ctx.file_access.put_data(archive_fname, full_remote_path)

        def fast_register_task(entity: GeneratedProtocolMessageType) -> GeneratedProtocolMessageType:
            """
            Updates task definitions during fast-registration in order to use the compatible pyflyte fast execute command at
            task execution.
            """
            # entity is of type flyteidl.admin.task_pb2.TaskSpec

            def _substitute_fast_register_task_args(args: List[str], full_remote_path: str, dest_dir: str) -> List[str]:
                complete_args = []
                for arg in args:
                    if arg == "{{ .remote_package_path }}":
                        arg = full_remote_path
                    elif arg == "{{ .dest_dir }}":
                        arg = dest_dir if dest_dir else "."
                    complete_args.append(arg)
                return complete_args

            if entity.template.HasField("container") and len(entity.template.container.args) > 0:
                complete_args = _substitute_fast_register_task_args(
                    entity.template.container.args, full_remote_path, destination_dir
                )
                # Because we're dealing with a proto list, we have to delete the existing args before we can extend the list
                # with the substituted ones.
                del entity.template.container.args[:]
                entity.template.container.args.extend(complete_args)

            if entity.template.HasField("k8s_pod"):
                pod_spec_struct = entity.template.k8s_pod.pod_spec
                if "containers" in pod_spec_struct:
                    for idx in range(len(pod_spec_struct["containers"])):
                        if "args" in pod_spec_struct["containers"][idx]:
                            # We can directly overwrite the args in the pod spec struct definition.
                            pod_spec_struct["containers"][idx]["args"] = _substitute_fast_register_task_args(
                                pod_spec_struct["containers"][idx]["args"], full_remote_path, destination_dir
                            )
            return entity

        patches = {
            identifier_pb2.TASK: fast_register_task,
            # _identifier_pb2.LAUNCH_PLAN: _get_patch_launch_plan_fn(
            #     assumable_iam_role, kubernetes_service_account, output_location_prefix
            # ),
        }

        # TODO: get this from the config file
        config_obj = configuration.PlatformConfig.auto()
        client = friendly.SynchronousFlyteClient(config_obj)
        # TODO: fix version in this call
        _extract_and_register(client, "flytesnacks", "development", version, pb_files, patches)
    else:
        click.secho(f"No flyte objects found in packages {pkgs}", fg="yellow")

    # TODO: Schedule execution using flyteremote
    # TODO: Dump flyteremote snippet to access the run
