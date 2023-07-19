import inspect
import json
import os
import runpy
import shlex
import subprocess
import typing
from contextlib import contextmanager
from pathlib import Path

import ipynbname
from IPython.core.magic import register_cell_magic, register_line_magic


def get_ipykernel_globals(stack: typing.List[inspect.FrameInfo]) -> typing.Optional[str]:
    for frame in stack:
        filepath = Path(frame.filename)
        if str(filepath.parent.name).startswith("ipykernel_"):
            return frame[0].f_globals
    raise ValueError("Could not find ipykernel globals")


@contextmanager
def convert_notebook(cell=None):
    nb_globals = get_ipykernel_globals(inspect.stack())

    if "__vsc_ipynb_file__" in nb_globals:
        notebook = nb_globals["__vsc_ipynb_file__"]
    else:
        notebook = Path(ipynbname.path()).name

    with open(notebook) as f:
        nb_raw = json.load(f)

    src = []
    for raw_cell in nb_raw["cells"]:
        if (
            raw_cell["cell_type"] != "code"
            or "notebook_mode" in "".join(raw_cell["source"])
            or "notebook_run" in "".join(raw_cell["source"])
            or "%load_ext run_notebook_remote" in "".join(raw_cell["source"])
        ):
            continue

        src.extend([*raw_cell["source"], "\n\n"])

    if cell is not None:
        notebook_mode_cell = ['if __name__ == "__main__":']
        for line in cell.split("\n"):
            notebook_mode_cell.append(f"{' '  * 4}{line}")
        src.append("\n".join(notebook_mode_cell))

    src = "".join(src)

    script = notebook.replace(".ipynb", ".py")
    with open(script, "w") as f:
        f.write(src)

    try:
        yield Path(script)
    finally:
        os.remove(script)


@register_line_magic
def notebook_run(line: str):
    with convert_notebook() as script:
        args = shlex.split(line)
        if args[0] == "--remote":
            args.insert(1, script.name)
        subprocess.run(["pyflyte", "run", *args])


@register_cell_magic
def notebook_mode(line, cell):
    with convert_notebook(cell) as script:
        runpy.run_module(script.stem, run_name="__main__")
