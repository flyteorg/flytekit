import datetime
import os

from flytekitplugins.papermill import NotebookTask
from flytekitplugins.pod import Pod
from kubernetes.client import V1Container, V1PodSpec

import flytekit
from flytekit import kwtypes
from flytekit.configuration import Image, ImageConfig
from flytekit.types.file import PythonNotebook

from .testdata.datatype import X


def _get_nb_path(name: str, suffix: str = "", abs: bool = True, ext: str = ".ipynb") -> str:
    """
    Creates a correct path no matter where the test is run from
    """
    _local_path = os.path.dirname(__file__)
    path = f"{_local_path}/testdata/{name}{suffix}{ext}"
    return os.path.abspath(path) if abs else path


def test_notebook_task_simple():
    nb_name = "nb-simple"
    nb = NotebookTask(
        name="test",
        notebook_path=_get_nb_path(nb_name, abs=False),
        inputs=kwtypes(pi=float),
        outputs=kwtypes(square=float),
    )
    sqr, out, render = nb.execute(pi=4)
    assert sqr == 16.0
    assert nb.python_interface.inputs == {"pi": float}
    assert nb.python_interface.outputs.keys() == {"square", "out_nb", "out_rendered_nb"}
    assert nb.output_notebook_path == out == _get_nb_path(nb_name, suffix="-out")
    assert nb.rendered_output_path == render == _get_nb_path(nb_name, suffix="-out", ext=".html")


def test_notebook_task_multi_values():
    nb_name = "nb-multi"
    nb = NotebookTask(
        name="test",
        notebook_path=_get_nb_path(nb_name, abs=False),
        inputs=kwtypes(x=int, y=int, h=str),
        outputs=kwtypes(z=int, m=int, h=str, n=datetime.datetime),
    )
    z, m, h, n, out, render = nb.execute(x=10, y=10, h="blah")
    assert z == 20
    assert m == 100
    assert h == "blah world!"
    assert type(n) == datetime.datetime
    assert nb.python_interface.inputs == {"x": int, "y": int, "h": str}
    assert nb.python_interface.outputs.keys() == {"z", "m", "h", "n", "out_nb", "out_rendered_nb"}
    assert nb.output_notebook_path == out == _get_nb_path(nb_name, suffix="-out")
    assert nb.rendered_output_path == render == _get_nb_path(nb_name, suffix="-out", ext=".html")


def test_notebook_task_complex():
    nb_name = "nb-complex"
    nb = NotebookTask(
        name="test",
        notebook_path=_get_nb_path(nb_name, abs=False),
        inputs=kwtypes(h=str, n=int, w=str),
        outputs=kwtypes(h=str, w=PythonNotebook, x=X),
    )
    h, w, x, out, render = nb.execute(h="blah", n=10, w=_get_nb_path("nb-multi"))
    assert h == "blah world!"
    assert w is not None
    assert x.x == 10
    assert nb.python_interface.inputs == {"n": int, "h": str, "w": str}
    assert nb.python_interface.outputs.keys() == {"h", "w", "x", "out_nb", "out_rendered_nb"}
    assert nb.output_notebook_path == out == _get_nb_path(nb_name, suffix="-out")
    assert nb.rendered_output_path == render == _get_nb_path(nb_name, suffix="-out", ext=".html")


def test_notebook_deck_local_execution_doesnt_fail():
    nb_name = "nb-simple"
    nb = NotebookTask(
        name="test",
        notebook_path=_get_nb_path(nb_name, abs=False),
        render_deck=True,
        inputs=kwtypes(pi=float),
        outputs=kwtypes(square=float),
    )
    sqr, out, render = nb.execute(pi=4)
    # This is largely a no assert test to ensure render_deck never inhibits local execution.
    assert nb._render_deck, "Passing render deck to init should result in private attribute being set"


def generate_por_spec_for_task():
    primary_container = V1Container(name="primary")
    pod_spec = V1PodSpec(containers=[primary_container])

    return pod_spec


nb = NotebookTask(
    name="test",
    task_config=Pod(pod_spec=generate_por_spec_for_task(), primary_container_name="primary"),
    notebook_path=_get_nb_path("nb-simple", abs=False),
    inputs=kwtypes(h=str, n=int, w=str),
    outputs=kwtypes(h=str, w=PythonNotebook, x=X),
)


def test_notebook_pod_task():
    serialization_settings = flytekit.configuration.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
    )

    assert nb.get_container(serialization_settings) is None
    assert nb.get_config(serialization_settings)["primary_container_name"] == "primary"
    assert (
        nb.get_command(serialization_settings)
        == nb.get_k8s_pod(serialization_settings).pod_spec["containers"][0]["args"]
    )
