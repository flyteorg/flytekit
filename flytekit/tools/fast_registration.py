from __future__ import annotations

import gzip
import hashlib
import os
import posixpath
import subprocess
import sys
import tarfile
import tempfile
import typing
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import click

from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.utils import timeit
from flytekit.exceptions.user import FlyteDataNotFoundException
from flytekit.loggers import logger
from flytekit.tools.ignore import DockerIgnore, FlyteIgnore, GitIgnore, Ignore, IgnoreGroup, StandardIgnore
from flytekit.tools.script_mode import ls_files, tar_strip_file_attributes

FAST_PREFIX = "fast"
FAST_FILEENDING = ".tar.gz"


class CopyFileDetection(Enum):
    LOADED_MODULES = 1
    ALL = 2


@dataclass(frozen=True)
class FastPackageOptions:
    """
    FastPackageOptions is used to set configuration options when packaging files.
    """

    ignores: list[Ignore]
    keep_default_ignores: bool = True
    copy_style: CopyFileDetection = CopyFileDetection.LOADED_MODULES


"""
clarify tar behavior
- tar doesn't seem to add the folder, just the files, but extracts okay
- this doesn't work for empty folders (but edge case because gitignore ignores them anyways?)
changes to tar
- will now add files one at a time.
- the list will compute the list and the digest
- could have used tar list but seems less powerful, also does more than we want.
still need to
- hook up the actual commands args
- in order to support auto, we have to load all the modules first, then trigger copying, then serialize post-upload
- make each create a separate tar
- have a separate path for old and new commands
process
- like to beta and update docs and test before deprecate
- so basically have to preserve both styles of code - worth it to do this? or just a long beta.
"""


def fast_package(
    source: os.PathLike,
    output_dir: os.PathLike,
    deref_symlinks: bool = False,
    options: Optional[FastPackageOptions] = None,
    # use_old: bool = False,
) -> os.PathLike:
    """
    Takes a source directory and packages everything not covered by common ignores into a tarball
    named after a hexdigest of the included files.
    :param os.PathLike source:
    :param os.PathLike output_dir:
    :param bool deref_symlinks: Enables dereferencing symlinks when packaging directory
    :return os.PathLike:
    """
    default_ignores = [GitIgnore, DockerIgnore, StandardIgnore, FlyteIgnore]
    if options is not None:
        if options.keep_default_ignores:
            ignores = options.ignores + default_ignores
        else:
            ignores = options.ignores
    else:
        ignores = default_ignores
    ignore = IgnoreGroup(source, ignores)

    digest = compute_digest(source, ignore.is_ignored)
    archive_fname = f"{FAST_PREFIX}{digest}{FAST_FILEENDING}"

    if output_dir is None:
        output_dir = tempfile.mkdtemp()
        click.secho(f"No output path provided, using a temporary directory at {output_dir} instead", fg="yellow")

    archive_fname = os.path.join(output_dir, archive_fname)

    if options and options.copy_style == CopyFileDetection.LOADED_MODULES:
        sys_modules = list(sys.modules.values())
        ls, ls_digest = ls_files(str(source), sys_modules, deref_symlinks, ignore)
    else:
        ls, ls_digest = ls_files(str(source), [], deref_symlinks, ignore)
    print(f"Digest check: old {digest} ==? new {ls_digest} -- {digest == ls_digest}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tar_path = os.path.join(tmp_dir, "tmp.tar")
        print(f"test tmp dir: {tar_path=}")
        with tarfile.open(tar_path, "w", dereference=True) as tar:
            for ws_file in ls:
                rel_path = os.path.relpath(ws_file, start=source)
                # not adding explicit folders, but extracting okay.
                tar.add(
                    os.path.join(source, ws_file),
                    arcname=rel_path,
                    filter=lambda x: tar_strip_file_attributes(x),
                )
            print("New tar file contents: ======================")
            tar.list(verbose=True)
        # breakpoint()

    with tempfile.TemporaryDirectory() as tmp_dir:
        tar_path = os.path.join(tmp_dir, "tmp.tar")
        with tarfile.open(tar_path, "w", dereference=deref_symlinks) as tar:
            files: typing.List[str] = os.listdir(source)
            for ws_file in files:
                tar.add(
                    os.path.join(source, ws_file),
                    arcname=ws_file,
                    filter=lambda x: ignore.tar_filter(tar_strip_file_attributes(x)),
                )
            print("Old tar file contents: ======================")
            tar.list(verbose=True)
        # breakpoint()
        with gzip.GzipFile(filename=archive_fname, mode="wb", mtime=0) as gzipped:
            with open(tar_path, "rb") as tar_file:
                gzipped.write(tar_file.read())

    return archive_fname


def compute_digest(source: os.PathLike, filter: Optional[callable] = None) -> str:
    """
    Walks the entirety of the source dir to compute a deterministic md5 hex digest of the dir contents.
    :param os.PathLike source:
    :param callable filter:
    :return Text:
    """
    hasher = hashlib.md5()
    for root, _, files in os.walk(source, topdown=True):
        files.sort()

        for fname in files:
            abspath = os.path.join(root, fname)
            # Only consider files that exist (e.g. disregard symlinks that point to non-existent files)
            if not os.path.exists(abspath):
                logger.info(f"Skipping non-existent file {abspath}")
                continue
            relpath = os.path.relpath(abspath, source)
            if filter:
                if filter(relpath):
                    continue

            _filehash_update(abspath, hasher)
            _pathhash_update(relpath, hasher)

    return hasher.hexdigest()


def _filehash_update(path: os.PathLike, hasher: hashlib._Hash) -> None:
    blocksize = 65536
    with open(path, "rb") as f:
        bytes = f.read(blocksize)
        while bytes:
            hasher.update(bytes)
            bytes = f.read(blocksize)


def _pathhash_update(path: os.PathLike, hasher: hashlib._Hash) -> None:
    path_list = path.split(os.sep)
    hasher.update("".join(path_list).encode("utf-8"))


def get_additional_distribution_loc(remote_location: str, identifier: str) -> str:
    """
    :param Text remote_location:
    :param Text identifier:
    :return Text:
    """
    return posixpath.join(remote_location, "{}.{}".format(identifier, "tar.gz"))


@timeit("Download distribution")
def download_distribution(additional_distribution: str, destination: str):
    """
    Downloads a remote code distribution and overwrites any local files.
    :param Text additional_distribution:
    :param os.PathLike destination:
    """
    if not os.path.isdir(destination):
        raise ValueError("Destination path is required to download distribution and it should be a directory")
    # NOTE the os.path.join(destination, ''). This is to ensure that the given path is in fact a directory and all
    # downloaded data should be copied into this directory. We do this to account for a difference in behavior in
    # fsspec, which requires a trailing slash in case of pre-existing directory.
    try:
        FlyteContextManager.current_context().file_access.get_data(
            additional_distribution, os.path.join(destination, "")
        )
    except FlyteDataNotFoundException as ex:
        raise RuntimeError("task execution code was not found") from ex
    tarfile_name = os.path.basename(additional_distribution)
    if not tarfile_name.endswith(".tar.gz"):
        raise RuntimeError("Unrecognized additional distribution format for {}".format(additional_distribution))

    # This will overwrite the existing user flyte workflow code in the current working code dir.
    result = subprocess.run(
        ["tar", "-xvf", os.path.join(destination, tarfile_name), "-C", destination],
        stdout=subprocess.PIPE,
    )
    result.check_returncode()
