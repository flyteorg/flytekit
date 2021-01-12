from flytekit import kwtypes
from flytekit.taskplugins.notebook import NotebookTask


def test_notebook_task():
    nb = NotebookTask(
        name="test", notebook_path="./test-notebook.ipynb", inputs=kwtypes(pi=float), outputs=kwtypes(square=float)
    )
    print(nb.execute(pi=4))
