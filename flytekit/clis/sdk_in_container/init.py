import rich_click as click

from flytekit.clis.sdk_in_container.helpers import clone_and_copy_repo_dir


@click.command("init")
@click.option(
    "--template",
    default="simple-example",
    help="cookiecutter template folder name to be used in the repo - https://github.com/flyteorg/flytekit-python-template.git",
)
@click.option(
    "--repository-url",
    default="https://github.com/flyteorg/flytekit-python-template.git",
    help="template repository url pointing to a git repository containing flytekit templates.",
)
@click.option("--repository-branch", default="main", help="template repository branch to be used.")
@click.argument("project-name")
def init(template, repository_url, repository_branch, project_name):
    """
    Create flyte-ready projects.
    """

    clone_and_copy_repo_dir(repository_url, repository_branch, template, project_name)

    click.echo(
        f"Visit the {project_name} directory and follow the next steps in the Getting started guide (https://docs.flyte.org/en/latest/getting_started.html) to proceed."
    )
