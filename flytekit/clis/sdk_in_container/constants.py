import click as _click

CTX_PROJECT = "project"
CTX_DOMAIN = "domain"
CTX_VERSION = "version"
CTX_TEST = "test"
CTX_PACKAGES = "pkgs"
CTX_NOTIFICATIONS = "notifications"
CTX_CONFIG_FILE = "config_file"
CTX_PROJECT_ROOT = "project_root"
CTX_MODULE = "module"


project_option = _click.option(
    "-p",
    "--project",
    required=True,
    type=str,
    help="Flyte project to use. You can have more than one project per repo",
)
domain_option = _click.option(
    "-d",
    "--domain",
    required=True,
    type=str,
    help="This is usually development, staging, or production",
)
version_option = _click.option(
    "-v",
    "--version",
    required=False,
    type=str,
    help="This is the version to apply globally for this context",
)
