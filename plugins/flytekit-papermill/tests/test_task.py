import datetime
import os

from flytekitplugins.papermill import NotebookTask

from flytekit import kwtypes
from flytekit.types.file import PythonNotebook
from plugins.tests.papermilltests.testdata.datatype import X


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
