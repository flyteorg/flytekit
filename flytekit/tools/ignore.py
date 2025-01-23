import os
import subprocess
import tarfile
from abc import ABC, abstractmethod
from fnmatch import fnmatch
from pathlib import Path
from shutil import which
from typing import List, Optional, Type

from docker.utils.build import PatternMatcher

from flytekit.loggers import logger

STANDARD_IGNORE_PATTERNS = ["*.pyc", ".cache", ".cache/*", "__pycache__/*", "**/__pycache__/*"]


class Ignore(ABC):
    """Base for Ignores, implements core logic. Children have to implement _is_ignored"""

    def __init__(self, root: str):
        self.root = root

    def is_ignored(self, path: str) -> bool:
        if os.path.isabs(path):
            path = os.path.relpath(path, self.root)
        return self._is_ignored(path)

    def tar_filter(self, tarinfo: tarfile.TarInfo) -> Optional[tarfile.TarInfo]:
        if self.is_ignored(tarinfo.name):
            return None
        return tarinfo

    @abstractmethod
    def _is_ignored(self, path: str) -> bool:
        pass


class GitIgnore(Ignore):
    """Uses git cli (if available) to list all ignored files and compare with those."""

    def __init__(self, root: Path):
        super().__init__(root)
        self.has_git = which("git") is not None
        self.ignored_files = self._list_ignored_files()
        self.ignored_dirs = self._list_ignored_dirs()

    def _git_wrapper(self, extra_args: List[str]) -> set[str]:
        if self.has_git:
            out = subprocess.run(
                ["git", "ls-files", "-io", "--exclude-standard", *extra_args],
                cwd=self.root,
                capture_output=True,
            )
            if out.returncode == 0:
                return set(out.stdout.decode("utf-8").split("\n")[:-1])
            logger.info(f"Could not determine ignored paths due to:\n{out.stderr}\nNot applying any filters")
            return set()
        logger.info("No git executable found, not applying any filters")
        return set()

    def _list_ignored_files(self) -> set[str]:
        return self._git_wrapper([])

    def _list_ignored_dirs(self) -> set[str]:
        return self._git_wrapper(["--directory"])

    def _is_ignored(self, path: str) -> bool:
        if self.ignored_files:
            # git-ls-files uses POSIX paths
            if Path(path).as_posix() in self.ignored_files:
                return True
            # Ignore empty directories
            if os.path.isdir(os.path.join(self.root, path)) and self.ignored_dirs:
                return Path(path).as_posix() + "/" in self.ignored_dirs
        return False


class DockerIgnore(Ignore):
    """Uses docker-py's PatternMatcher to check whether a path is ignored."""

    def __init__(self, root: Path):
        super().__init__(root)
        self.pm = self._parse()

    def _parse(self) -> PatternMatcher:
        patterns = []
        dockerignore = os.path.join(self.root, ".dockerignore")
        if os.path.isfile(dockerignore):
            with open(dockerignore, "r") as f:
                patterns = [l.strip() for l in f.readlines() if l and not l.startswith("#")]
        else:
            logger.info(f"No .dockerignore found in {self.root}, not applying any filters")
        return PatternMatcher(patterns)

    def _is_ignored(self, path: str) -> bool:
        return self.pm.matches(path)


class FlyteIgnore(Ignore):
    """Uses a .flyteignore file to determine ignored files."""

    def __init__(self, root: Path):
        super().__init__(root)
        self.pm = self._parse()

    def _parse(self) -> PatternMatcher:
        patterns = []
        flyteignore = os.path.join(self.root, ".flyteignore")
        if os.path.isfile(flyteignore):
            with open(flyteignore, "r") as f:
                patterns = [l.strip() for l in f.readlines() if l and not l.startswith("#")]
        else:
            logger.info(f"No .flyteignore found in {self.root}, not applying any filters")
        return PatternMatcher(patterns)

    def _is_ignored(self, path: str) -> bool:
        return self.pm.matches(path)


class StandardIgnore(Ignore):
    """Retains the standard ignore functionality that previously existed. Could in theory
    by fed with custom ignore patterns from cli."""

    def __init__(self, root: Path, patterns: Optional[List[str]] = None):
        super().__init__(root)
        self.patterns = patterns if patterns else STANDARD_IGNORE_PATTERNS

    def _is_ignored(self, path: str) -> bool:
        for pattern in self.patterns:
            if fnmatch(path, pattern):
                return True
        return False


class IgnoreGroup(Ignore):
    """Groups multiple Ignores and checks a path against them. A file is ignored if any
    Ignore considers it ignored."""

    def __init__(self, root: str, ignores: List[Type[Ignore]]):
        super().__init__(root)
        self.ignores = [ignore(root) for ignore in ignores]

    def _is_ignored(self, path: str) -> bool:
        for ignore in self.ignores:
            if ignore.is_ignored(path):
                return True
        return False

    def list_ignored(self) -> List[str]:
        ignored = []
        for root, _, files in os.walk(self.root):
            for file in files:
                abs_path = os.path.join(root, file)
                if self.is_ignored(abs_path):
                    ignored.append(os.path.relpath(abs_path, self.root))
        return ignored
