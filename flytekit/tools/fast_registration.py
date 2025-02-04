from __future__ import annotations

import gzip
import hashlib
import os
import pathlib
import posixpath
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time
import typing
from dataclasses import dataclass
from typing import List, Optional, Union

import click
from rich import print as rich_print
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)
from rich.tree import Tree

from flytekit.constants import CopyFileDetection
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.python_auto_container import PICKLE_FILE_PATH
from flytekit.core.utils import timeit
from flytekit.exceptions.user import FlyteDataNotFoundException
from flytekit.loggers import is_display_progress_enabled, logger
from flytekit.tools.ignore import DockerIgnore, FlyteIgnore, GitIgnore, Ignore, IgnoreGroup, StandardIgnore
from flytekit.tools.script_mode import _filehash_update, _pathhash_update, ls_files, tar_strip_file_attributes

FAST_PREFIX = "fast"
FAST_FILEENDING = ".tar.gz"


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
                current_path = current_path / subdir
                if current_path not in trees:
                    current = current.add(f"{subdir}", guide_style="bold bright_blue")
                    trees[current_path] = current
                else:
                    current = trees[current_path]
        trees[fpp.parent].add(f"{fpp.name}", guide_style="bold bright_blue")
    rich_print(tree_root)


def compress_tarball(source: os.PathLike, output: os.PathLike) -> None:
    """Compress code tarball using pigz if available, otherwise gzip"""
    if pigz := shutil.which("pigz"):
        with open(output, "wb") as gzipped:
            subprocess.run([pigz, "--no-time", "-c", source], stdout=gzipped, check=True)
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

    # This function is temporarily split into two, to support the creation of the tar file in both the old way,
    # copying the underlying items in the source dir by doing a listdir, and the new way, relying on a list of files.
    if options and (
        options.copy_style == CopyFileDetection.LOADED_MODULES or options.copy_style == CopyFileDetection.ALL
    ):
        create_tarball_progress = Progress(
            TimeElapsedColumn(),
            TextColumn("[progress.description]{task.description}."),
            BarColumn(),
            TextColumn("{task.fields[files_added_progress]}"),
        )

        compress_tarball_progress = Progress(
            TimeElapsedColumn(),
            TextColumn("[progress.description]{task.description}"),
        )

        ls, ls_digest = ls_files(str(source), options.copy_style, deref_symlinks, ignore)
        logger.debug(f"Hash digest: {ls_digest}")

        if options.show_files:
            print_ls_tree(source, ls)

        # Compute where the archive should be written
        archive_fname = f"{FAST_PREFIX}{ls_digest}{FAST_FILEENDING}"
        if output_dir is None:
            output_dir = tempfile.mkdtemp()
            click.secho(
                f"No output path provided, using a temporary directory at {output_dir} instead",
                fg="yellow",
            )
        archive_fname = os.path.join(output_dir, archive_fname)

        # add the tarfile task to progress and start it
        total_files = len(ls)
        files_processed = 0
        tar_task = create_tarball_progress.add_task(
            f"Creating tarball with [{total_files}] files...",
            total=total_files,
            files_added_progress=f"{files_processed}/{total_files} files",
        )

        if is_display_progress_enabled():
            create_tarball_progress.start()

        create_tarball_progress.start_task(tar_task)
        with tempfile.TemporaryDirectory() as tmp_dir:
            tar_path = os.path.join(tmp_dir, "tmp.tar")
            with tarfile.open(tar_path, "w", dereference=deref_symlinks) as tar:
                for ws_file in ls:
                    files_processed = files_processed + 1
                    rel_path = os.path.relpath(ws_file, start=source)
                    tar.add(
                        os.path.join(source, ws_file),
                        recursive=False,
                        arcname=rel_path,
                        filter=lambda x: tar_strip_file_attributes(x),
                    )

                    create_tarball_progress.update(
                        tar_task,
                        advance=1,
                        description=f"Added file {rel_path}",
                        refresh=True,
                        files_added_progress=f"{files_processed}/{total_files} files",
                    )

            create_tarball_progress.stop_task(tar_task)
            if is_display_progress_enabled():
                create_tarball_progress.stop()
                compress_tarball_progress.start()

            tpath = pathlib.Path(tar_path)
            size_mbs = tpath.stat().st_size / 1024 / 1024
            compress_task = compress_tarball_progress.add_task(f"Compressing tarball size {size_mbs:.2f}MB...", total=1)
            compress_tarball_progress.start_task(compress_task)
            compress_tarball(tar_path, archive_fname)
            arpath = pathlib.Path(archive_fname)
            asize_mbs = arpath.stat().st_size / 1024 / 1024
            compress_tarball_progress.update(
                compress_task,
                advance=1,
                description=f"Tarball {size_mbs:.2f}MB compressed to {asize_mbs:.2f}MB",
            )
            compress_tarball_progress.stop_task(compress_task)
            if is_display_progress_enabled():
                compress_tarball_progress.stop()

    # Original tar command - This condition to be removed in the future after serialize is removed.
    else:
        # Remove this after original tar command is removed.
        digest = compute_digest(source, ignore.is_ignored)

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

            compress_tarball(tar_path, archive_fname)

    return archive_fname


def compute_digest(source: Union[os.PathLike, List[os.PathLike]], filter: Optional[callable] = None) -> str:
    """
    Walks the entirety of the source dir to compute a deterministic md5 hex digest of the dir contents.
    :param os.PathLike source:
    :param callable filter:
    :return Text:
    """
    hasher = hashlib.md5()

    def compute_digest_for_file(path: os.PathLike, rel_path: os.PathLike) -> None:
        # Only consider files that exist (e.g. disregard symlinks that point to non-existent files)
        if not os.path.exists(path):
            logger.info(f"Skipping non-existent file {path}")
            return

        # Skip socket files
        if stat.S_ISSOCK(os.stat(path).st_mode):
            logger.info(f"Skip socket file {path}")
            return

        if filter:
            if filter(rel_path):
                return

        _filehash_update(path, hasher)
        _pathhash_update(rel_path, hasher)

    def compute_digest_for_dir(source: os.PathLike) -> None:
        for root, _, files in os.walk(source, topdown=True):
            files.sort()

            for fname in files:
                abspath = os.path.join(root, fname)
                relpath = os.path.relpath(abspath, source)
                compute_digest_for_file(abspath, relpath)

    if isinstance(source, list):
        for src in source:
            if os.path.isdir(src):
                compute_digest_for_dir(src)
            else:
                compute_digest_for_file(src, os.path.basename(src))
    else:
        compute_digest_for_dir(source)

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
    if tarfile_name.endswith(".tar.gz"):
        # This will overwrite the existing user flyte workflow code in the current working code dir.
        result = subprocess.run(
            ["tar", "-xvf", os.path.join(destination, tarfile_name), "-C", destination],
            stdout=subprocess.PIPE,
        )
        result.check_returncode()
    elif tarfile_name != PICKLE_FILE_PATH:
        # The distribution is not a pickled file.
        raise RuntimeError("Unrecognized additional distribution format for {}".format(additional_distribution))
