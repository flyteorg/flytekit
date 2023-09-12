import typing
from dataclasses import Field, dataclass, field
from types import MappingProxyType

import click
import rich_click as _click

CTX_PROJECT = "project"
CTX_DOMAIN = "domain"
CTX_VERSION = "version"
CTX_TEST = "test"
CTX_PACKAGES = "pkgs"
CTX_NOTIFICATIONS = "notifications"
CTX_CONFIG_FILE = "config_file"
CTX_VERBOSE = "verbose"


def make_field(o: click.Option) -> Field:
    if o.multiple:
        o.help = click.style("Multiple values allowed.", bold=True) + f"{o.help}"
        return field(default_factory=lambda: o.default, metadata={"click.option": o})
    return field(default=o.default, metadata={"click.option": o})


def get_option_from_metadata(metadata: MappingProxyType) -> click.Option:
    return metadata["click.option"]


@dataclass
class PyFlyteParams:
    config_file: typing.Optional[str] = None
    verbose: bool = False
    pkgs: typing.List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: typing.Dict[str, typing.Any]) -> "PyFlyteParams":
        return cls(**d)


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
