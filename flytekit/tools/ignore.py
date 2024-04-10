import os
import re
import subprocess
import sys
import tarfile as _tarfile
from abc import ABC, abstractmethod
from fnmatch import fnmatch
from pathlib import Path
from shutil import which
from typing import Dict, List, Optional, Type

from flytekit.loggers import logger

IS_WINDOWS_PLATFORM = sys.platform == "win32"

STANDARD_IGNORE_PATTERNS = ["*.pyc", ".cache", ".cache/*", "__pycache__", "**/__pycache__"]
_SEP = re.compile("/|\\\\") if IS_WINDOWS_PLATFORM else re.compile("/")


def split_path(p):
    return [pt for pt in re.split(_SEP, p) if pt and pt != "."]


def normalize_slashes(p):
    if IS_WINDOWS_PLATFORM:
        return "/".join(split_path(p))
    return p


# It's copied from docker module to reduce flytekit module size.
# reference: https://github.com/docker/docker-py/blob/9ad4bddc9ee23f3646f256280a21ef86274e39bc/docker/utils/build.py#L159
class PatternMatcher:
    def __init__(self, patterns):
        self.patterns = list(filter(lambda p: p.dirs, [Pattern(p) for p in patterns]))
        self.patterns.append(Pattern("!.dockerignore"))

    def matches(self, filepath):
        matched = False
        parent_path = os.path.dirname(filepath)
        parent_path_dirs = split_path(parent_path)

        for pattern in self.patterns:
            negative = pattern.exclusion
            match = pattern.match(filepath)
            if not match and parent_path != "":
                if len(pattern.dirs) <= len(parent_path_dirs):
                    match = pattern.match(os.path.sep.join(parent_path_dirs[: len(pattern.dirs)]))

            if match:
                matched = not negative

        return matched

    def walk(self, root):
        def rec_walk(current_dir):
            for f in os.listdir(current_dir):
                fpath = os.path.join(os.path.relpath(current_dir, root), f)
                if fpath.startswith("." + os.path.sep):
                    fpath = fpath[2:]
                match = self.matches(fpath)
                if not match:
                    yield fpath

                cur = os.path.join(root, fpath)
                if not os.path.isdir(cur) or os.path.islink(cur):
                    continue

                if match:
                    # If we want to skip this file and it's a directory
                    # then we should first check to see if there's an
                    # excludes pattern (e.g. !dir/file) that starts with this
                    # dir. If so then we can't skip this dir.
                    skip = True

                    for pat in self.patterns:
                        if not pat.exclusion:
                            continue
                        if pat.cleaned_pattern.startswith(normalize_slashes(fpath)):
                            skip = False
                            break
                    if skip:
                        continue
                yield from rec_walk(cur)

        return rec_walk(root)


class Pattern:
    def __init__(self, pattern_str):
        self.exclusion = False
        if pattern_str.startswith("!"):
            self.exclusion = True
            pattern_str = pattern_str[1:]

        self.dirs = self.normalize(pattern_str)
        self.cleaned_pattern = "/".join(self.dirs)

    @classmethod
    def normalize(cls, p):
        # Remove trailing spaces
        p = p.strip()

        # Leading and trailing slashes are not relevant. Yes,
        # "foo.py/" must exclude the "foo.py" regular file. "."
        # components are not relevant either, even if the whole
        # pattern is only ".", as the Docker reference states: "For
        # historical reasons, the pattern . is ignored."
        # ".." component must be cleared with the potential previous
        # component, regardless of whether it exists: "A preprocessing
        # step [...]  eliminates . and .. elements using Go's
        # filepath.".
        i = 0
        split = split_path(p)
        while i < len(split):
            if split[i] == "..":
                del split[i]
                if i > 0:
                    del split[i - 1]
                    i -= 1
            else:
                i += 1
        return split

    def match(self, filepath):
        return fnmatch(normalize_slashes(filepath), self.cleaned_pattern)


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


class GitIgnore(Ignore):
    """Uses git cli (if available) to list all ignored files and compare with those."""

    def __init__(self, root: Path):
        super().__init__(root)
        self.has_git = which("git") is not None
        self.ignored = self._list_ignored()

    def _list_ignored(self) -> Dict:
        if self.has_git:
            out = subprocess.run(["git", "ls-files", "-io", "--exclude-standard"], cwd=self.root, capture_output=True)
            if out.returncode == 0:
                return dict.fromkeys(out.stdout.decode("utf-8").split("\n")[:-1])
            logger.warning(f"Could not determine ignored files due to:\n{out.stderr}\nNot applying any filters")
            return {}
        logger.info("No git executable found, not applying any filters")
        return {}

    def _is_ignored(self, path: str) -> bool:
        if self.ignored:
            # git-ls-files uses POSIX paths
            if Path(path).as_posix() in self.ignored:
                return True
            # Ignore empty directories
            if os.path.isdir(os.path.join(self.root, path)) and all(
                [self.is_ignored(os.path.join(path, f)) for f in os.listdir(os.path.join(self.root, path))]
            ):
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
        logger.info(f"No .dockerignore found in {self.root}, not applying any filters")
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
