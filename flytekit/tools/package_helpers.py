import os
import tarfile
import checksumdir
from typing import List, Optional

from flytekit.tools.ignore import DockerIgnore, GitIgnore, IgnoreGroup, StandardIgnore

FAST_PREFIX = "fast"
FAST_FILEENDING = ".tar.gz"

def compute_digest(source_dir: os.PathLike, excluded_files: Optional[List[os.PathLike]] = None) -> str:
    """
    Walks the entirety of the source dir to compute a deterministic hex digest of the dir contents.
    :param _os.PathLike source_dir:
    :return Text:
    """
    return checksumdir.dirhash(source_dir, 'md5', include_paths=True, excluded_files=excluded_files)


def create_archive(source: os.PathLike, output_dir: Optional[os.PathLike] = None) -> os.PathLike:
    ignore = IgnoreGroup(source, [GitIgnore, DockerIgnore, StandardIgnore])
    ignored_files = ignore.list_ignored()
    digest = compute_digest(source, ignored_files)
    archive_fname = f"{FAST_PREFIX}{digest}{FAST_FILEENDING}"

    if output_dir:
        archive_fname = os.path.join(output_dir, archive_fname)

    with tarfile.open(archive_fname, "w:gz") as tar:
        tar.add(source, arcname="", filter=ignore.tar_filter)
    return archive_fname
