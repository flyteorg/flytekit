import tarfile

from flytekit.tools.ignore import DockerIgnore, GitIgnore, IgnoreGroup, StandardIgnore


def create_archive(source: str, name: str) -> None:
    ignore = IgnoreGroup(source, [GitIgnore, DockerIgnore, StandardIgnore])
    with tarfile.open(name, "w:gz") as tar:
        tar.add(source, arcname="", filter=ignore.tar_filter)
