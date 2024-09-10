from __future__ import annotations

import gzip
import hashlib
import os
import pathlib
import posixpath
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import typing
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import click
from rich import print as rich_print
from rich.tree import Tree

from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.utils import timeit
from flytekit.exceptions.user import FlyteDataNotFoundException
from flytekit.loggers import logger
from flytekit.tools.ignore import DockerIgnore, FlyteIgnore, GitIgnore, Ignore, IgnoreGroup, StandardIgnore
from flytekit.tools.script_mode import _filehash_update, _pathhash_update, ls_files, tar_strip_file_attributes

FAST_PREFIX = "fast"
FAST_FILEENDING = ".tar.gz"


class CopyFileDetection(Enum):
    LOADED_MODULES = 1
    ALL = 2
    # This option's meaning will change in the future. In the future this will mean that no files should be copied
    # (i.e. no fast registration is used). For now, both this value and setting this Enum to Python None are both
    # valid to distinguish between users explicitly setting --copy none and not setting the flag.
    # Currently, this is only used for register, not for package or run because run doesn't have a no-fast-register
    # option and package is by default non-fast.
    NO_COPY = 3


@dataclass(frozen=True)
class FastPackageOptions:
    """
    FastPackageOptions is used to set configuration options when packaging files.
    """

    ignores: list[Ignore]
    keep_default_ignores: bool = True
    copy_style: Optional[CopyFileDetection] = None
    show_files: bool = False


def print_ls_tree(source: os.PathLike, ls: typing.List[str]):
    click.secho("Files to be copied for fast registration...", fg="bright_blue")

    tree_root = Tree(
        f":open_file_folder: [link file://{source}]{source} (detected source root)",
        guide_style="bold bright_blue",
    )
    trees = {pathlib.Path(source): tree_root}

    for f in ls:
        fpp = pathlib.Path(f)
        if fpp.parent not in trees:
            # add trees for all intermediate folders
            current = tree_root
            current_path = pathlib.Path(source)
            for subdir in fpp.parent.relative_to(source).parts:
                current = current.add(f"{subdir}", guide_style="bold bright_blue")
                current_path = current_path / subdir
                trees[current_path] = current
        trees[fpp.parent].add(f"{fpp.name}", guide_style="bold bright_blue")
    rich_print(tree_root)


def compress_tarball(source: os.PathLike, output: os.PathLike) -> None:
    """Compress code tarball using pigz if available, otherwise gzip"""
    if pigz := shutil.which("pigz"):
        with open(output, "wb") as gzipped:
            subprocess.run([pigz, "-c", source], stdout=gzipped, check=True)
    else:
        start_time = time.time()
        with gzip.GzipFile(filename=output, mode="wb", mtime=0) as gzipped:
            with open(source, "rb") as source_file:
                gzipped.write(source_file.read())

        end_time = time.time()
        warning_time = 10
        if end_time - start_time > warning_time:
            click.secho(
                f"Code tarball compression took {end_time - start_time:.0f} seconds. Consider installing `pigz` for faster compression.",
                fg="yellow",
            )


def fast_package(
    source: os.PathLike,
    output_dir: os.PathLike,
    deref_symlinks: bool = False,
    options: Optional[FastPackageOptions] = None,
) -> os.PathLike:
    """
    Takes a source directory and packages everything not covered by common ignores into a tarball
    named after a hexdigest of the included files.
    :param os.PathLike source:
    :param os.PathLike output_dir:
    :param bool deref_symlinks: Enables dereferencing symlinks when packaging directory
    :param options: The CopyFileDetection option set to None
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

    # Remove this after original tar command is removed.
    digest = compute_digest(source, ignore.is_ignored)

    # This function is temporarily split into two, to support the creation of the tar file in both the old way,
    # copying the underlying items in the source dir by doing a listdir, and the new way, relying on a list of files.
    if options and (
        options.copy_style == CopyFileDetection.LOADED_MODULES or options.copy_style == CopyFileDetection.ALL
    ):
        if options.copy_style == CopyFileDetection.LOADED_MODULES:
            # This is the 'auto' semantic by default used for pyflyte run, it only copies loaded .py files.
            sys_modules = list(sys.modules.values())
            ls, ls_digest = ls_files(str(source), sys_modules, deref_symlinks, ignore)
        else:
            # This triggers listing of all files, mimicking the old way of creating the tar file.
            ls, ls_digest = ls_files(str(source), [], deref_symlinks, ignore)

        logger.debug(f"Hash digest: {ls_digest}", fg="green")

        if options.show_files:
            print_ls_tree(source, ls)

        # Compute where the archive should be written
        archive_fname = f"{FAST_PREFIX}{ls_digest}{FAST_FILEENDING}"
        if output_dir is None:
            output_dir = tempfile.mkdtemp()
            click.secho(f"No output path provided, using a temporary directory at {output_dir} instead", fg="yellow")
        archive_fname = os.path.join(output_dir, archive_fname)

        with tempfile.TemporaryDirectory() as tmp_dir:
            tar_path = os.path.join(tmp_dir, "tmp.tar")
            with tarfile.open(tar_path, "w", dereference=True) as tar:
                for ws_file in ls:
                    rel_path = os.path.relpath(ws_file, start=source)
                    tar.add(
                        os.path.join(source, ws_file),
                        arcname=rel_path,
                        filter=lambda x: tar_strip_file_attributes(x),
                    )

            compress_tarball(tar_path, archive_fname)

    # Original tar command - This condition to be removed in the future.
    else:
        # Compute where the archive should be written
        archive_fname = f"{FAST_PREFIX}{digest}{FAST_FILEENDING}"
        if output_dir is None:
            output_dir = tempfile.mkdtemp()
            click.secho(f"No output path provided, using a temporary directory at {output_dir} instead", fg="yellow")
        archive_fname = os.path.join(output_dir, archive_fname)

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
                # tar.list(verbose=True)

            compress_tarball(tar_path, archive_fname)

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
