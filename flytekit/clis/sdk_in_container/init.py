import click
from cookiecutter.main import cookiecutter
from cookiecutter.prompt import read_user_variable


@click.group("init")
def init():
    """
    Create flyte-ready projects.
    """
    # TODO: Add `interactive` boolean flag to allow for programmatic access to this subcommand.
    pass


@click.command("example")
def generate_simple_example():
    """
    This command creates a directory containing the minimal code necessary to start a Flyte-ready project.
    The generated directory structure for the default values resemble:
        flyte_example
        ├── Dockerfile
        ├── flyte.config
        ├── LICENSE
        ├── myapp
        │   ├── __init__.py
        │   └── workflows
        │       ├── example.py
        │       └── __init__.py
        ├── README.md
        └── requirements.txt
    """
    print("What is the name of your project? This is the name of the directory that will be created.")
    project_name = read_user_variable("project_name", "flyte_example")
    print("What should we call your application? This serves as the top level package where your workflows will live.")
    app = read_user_variable("app", "myapp")
    print("What should be the name of your example workflow?")
    workflow_name = read_user_variable("workflow", "workflow_example")

    config = {
        "project_name": project_name,
        "app": app,
        "workflow": workflow_name,
    }
    cookiecutter(
        "https://github.com/flyteorg/flytekit-python-template.git",
        # TODO: remove this once we make the transition to cookie-cutter official.
        checkout="cookie-cutter",
        no_input=True,
        # We do not want to clobber existing files/directories.
        overwrite_if_exists=False,
        extra_context=config,
        # By specifying directory we can have multiple templates in the same repository,
        # as described in https://cookiecutter.readthedocs.io/en/1.7.2/advanced/directories.html.
        # The idea is to extend the number of templates, each in their own subdirectory, for example
        # a tensorflow-based example.
        directory="simple-example",
    )


init.add_command(generate_simple_example)
