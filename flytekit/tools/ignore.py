import os
import subprocess
import tarfile as _tarfile
from abc import ABC, abstractmethod
from fnmatch import fnmatch
from pathlib import Path
from shutil import which
from typing import List, Optional

from docker.utils.build import PatternMatcher

from flytekit.loggers import cli_logger

STANDARD_IGNORE_PATTERNS = ["*.pyc", ".cache", ".cache/*", "__pycache__", "**/__pycache__"]


class Ignore(ABC):
    """Base for Ignores, implements core logic. Children have to implement _is_ignored"""

    def __init__(self, root: str):
        self.root = root

    def is_ignored(self, path: str) -> bool:
        if os.path.isabs(path):
            path = os.path.relpath(path, self.root)
        return self._is_ignored(path)

    def tar_filter(self, tarinfo: _tarfile.TarInfo) -> Optional[_tarfile.TarInfo]:
        if self.is_ignored(tarinfo.name):
            return None
        return tarinfo

    @abstractmethod
    def _is_ignored(self, path: str) -> bool:
        pass


class GitIgnoreV1(Ignore):
    """Uses git cli (if available) to check whether a path is ignored."""

    def __init__(self, root: Path):
        super().__init__(root)
        self.has_git = which("git") is not None

    def _is_ignored(self, path: str) -> bool:
        if self.has_git:
            out = subprocess.run(["git", "check-ignore", path], cwd=self.root)
            # Returncode is 0 if file is ignored and 1 if otherwise
            return not out.returncode
        cli_logger.info(f"No git executable found, not applying any filters")
        return False


class GitIgnore(Ignore):
    """Uses git cli (if available) to check whether a path is ignored."""

    def __init__(self, root: Path):
        super().__init__(root)
        self.has_git = which("git") is not None
        self.ignored = self._list_ignored()

    def _list_ignored(self) -> List[str]:
        if self.has_git:
            out = subprocess.run(["git", "ls-files", "-io", "--exclude-standard"], cwd=self.root, capture_output=True)
            if out.returncode == 0:
                return out.stdout.decode("utf-8").split("\n")[:-1]
            cli_logger.warning(f"Could not determine ignored files due to:\n{out.stderr}\nNot applying any filters")
            return []
        cli_logger.info("No git executable found, not applying any filters")
        return []

    def _is_ignored(self, path: str) -> bool:
        if self.ignored:
            if path in self.ignored:
                return True
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
        cli_logger.info(f"No .dockerignore found in {self.root}, not applying any filters")
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

    def __init__(self, root: str, ignores: List[Ignore]):
        self.ignores = [ignore(root) for ignore in ignores]

    def _is_ignored(self, path: str) -> bool:
        for ignore in self.ignores:
            if ignore.is_ignored(path):
                return True
        return False
