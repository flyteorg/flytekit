import os
from collections import OrderedDict
from pathlib import Path

import click

from flytekit.clis.sdk_in_container.run import load_naive_entity
from flytekit.configuration import SerializationSettings, ImageConfig
from flytekit.core.base_task import PythonTask
from flytekit.extend.image_spec.base_image import build_docker_image
from flytekit.tools.script_mode import _find_project_root, hash_file
from flytekit.tools.translator import get_serializable

_build_help = """Build a image for the flyte task or workflow."""


# @click.command("build", help=_build_help)
# @click.option(
#     "--file",
#     required=True,
#     type=str,
#     help="task or workflow file",
# )
# @click.option(
#     "--name",
#     required=True,
#     type=str,
#     help="task or workflow name",
# )
# @click.pass_context
def build(_: click.Context, file: str, name: str):
    """
    $ pyflyte build wf.py t1  # build the images in the task decorator

    @task(image_spec=ImageSpec(...))
    def t1():
        ...

    In this case, pyflyte will build a image for t1, and push it to the remote registry.
    """
    m = OrderedDict()
    file = Path(file).resolve()
    _, str_digest = hash_file(file)
    project_root = _find_project_root(file)
    rel_path = Path(file).relative_to(project_root)
    module = os.path.splitext(rel_path)[0].replace(os.path.sep, ".")

    entity = load_naive_entity(module, name, project_root)
    for n in entity.nodes:
        print(n.name)
        print(n.flyte_entity.image_spec)
        image_name = f"{n.name}:{str_digest}"
        # build_docker_image(n.flyte_entity.image_spec, name=image_name)

    # serialization_settings = SerializationSettings(
    #     ImageConfig.auto_default_image(),
    #     project="flytesnacks",
    #     domain="development",
    #     version="t1",
    # )
    # _ = get_serializable(m, settings=serialization_settings, entity=entity, options=None)
    # # print(m)
    # for entity in m:
    #     if isinstance(entity, PythonTask):
    #         print(entity.get_container(serialization_settings).image)


if __name__ == '__main__':
    build(None, "/Users/kevin/git/flytekit/flyte-example/example_test.py", "wf")
