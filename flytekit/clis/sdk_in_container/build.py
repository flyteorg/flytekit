import os
import pathlib
from collections import OrderedDict
from pathlib import Path

import click

from flytekit.clis.sdk_in_container.run import get_entities_in_file, load_naive_entity
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.extend.image_spec.base_image import build_docker_image, calculate_hash_from_image_spec
from flytekit.tools.script_mode import _find_project_root, hash_file
from flytekit.tools.translator import get_serializable

_build_help = """Build a image for the flyte task or workflow."""


@click.command("build", help=_build_help)
@click.option(
    "--file",
    required=True,
    type=str,
    help="task or workflow file",
)
@click.pass_context
def build(_: click.Context, file: str):
    """
    $ pyflyte build --file wf.py

    Parse all the imageSpecs in this file, and build the image.

    @task(image_spec=ImageSpec(...))
    def t1():
        ...

    """
    rel_path = os.path.relpath(file)
    entities = get_entities_in_file(pathlib.Path(file).resolve())  # type: ignore
    project_root = _find_project_root(file)

    for entity in entities.all():
        module = os.path.splitext(rel_path)[0].replace(os.path.sep, ".")
        exe_entity = load_naive_entity(module, entity, project_root)
        image_spec = exe_entity.image_spec
        if image_spec is None:
            continue
        image_name = f"{image_spec.registry}/flytekit"
        print(image_spec)
        print(image_name)
        print(calculate_hash_from_image_spec(image_spec))
        build_docker_image(image_spec, name=image_name, tag=calculate_hash_from_image_spec(image_spec))
