import os
import re
from io import BytesIO
from zipfile import ZipFile

import requests
import rich_click as click


@click.command("init")
@click.option(
    "--template",
    default="basic-template-imagespec",
    help="template folder name to be used in the repo - https://github.com/flyteorg/flytekit-python-template.git",
)
@click.argument("project-name")
def init(template, project_name):
    """
    Create flyte-ready projects.
    """
    if os.path.exists(project_name):
        raise click.ClickException(f"{project_name} directory already exists")

    template_zip_url = "https://github.com/flyteorg/flytekit-python-template/archive/refs/heads/main.zip"

    response = requests.get(template_zip_url)

    if response.status_code != 200:
        raise click.ClickException("Unable to download template from github.com/flyteorg/flytekit-python-template")

    zip_content = BytesIO(response.content)
    zip_root_name = "flytekit-python-template-main"
    project_name_template = "{{cookiecutter.project_name}}"
    prefix = os.path.join(zip_root_name, template, project_name_template, "")
    prefix_len = len(prefix)

    # We use a regex here to be more compatible with cookiecutter templating
    project_template_regex = re.compile(rb"\{\{ ?cookiecutter\.project_name ?\}\}")

    project_name_bytes = project_name.encode("utf-8")

    with ZipFile(zip_content, "r") as zip_file:
        template_members = [m for m in zip_file.namelist() if m.startswith(prefix)]

        for member in template_members:
            dest = os.path.join(project_name, member[prefix_len:])

            # member is a directory
            if dest.endswith(os.sep):
                if not os.path.exists(dest):
                    os.mkdir(dest)
                continue

            # member is a file
            with zip_file.open(member) as zip_member, open(dest, "wb") as dest_file:
                zip_contents = zip_member.read()
                processed_contents = project_template_regex.sub(project_name_bytes, zip_contents)
                dest_file.write(processed_contents)

    click.echo(
        f"Visit the {project_name} directory and follow the next steps in the Getting started guide (https://docs.flyte.org/en/latest/getting_started_with_workflow_development/index.html) to proceed."
    )
