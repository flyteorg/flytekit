import json
import os
import subprocess

import click


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
    )
)
@click.option("--remote", is_flag=True, default=False)
@click.argument("notebook")
@click.argument("pyflyte_run_args", nargs=-1, type=click.UNPROCESSED)
def run_notebook(remote, notebook, pyflyte_run_args):
    with open(notebook) as f:
        nb_raw = json.load(f)

    src = []
    for cell in nb_raw["cells"]:
        if (
            cell["cell_type"] != "code"
            # remove notebook-mode cells
            or "%%notebook_mode" in "".join(cell["source"])
        ):
            continue

        # remove notebook-mode import
        lines = [l for l in cell["source"] if "notebook_mode" not in l]
        src.extend([*lines, "\n\n"])
    src = "".join(src)

    script = notebook.replace(".ipynb", ".py")
    with open(script, "w") as f:
        f.write(src)

    args = ["--remote"] if remote else []
    args.extend([script, *pyflyte_run_args])
    try:
        subprocess.run(["pyflyte", "run", *args])
    finally:
        os.remove(script)


if __name__ == "__main__":
    run_notebook()
