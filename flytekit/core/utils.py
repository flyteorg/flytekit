import os as _os
import shutil as _shutil
import tempfile as _tempfile
import time as _time
from hashlib import sha224 as _sha224
from pathlib import Path
from typing import Dict, List, Optional

from flytekit.loggers import logger
from flytekit.models import task as _task_models


def _dnsify(value: str) -> str:
    """
    Converts value into a DNS-compliant (RFC1035/RFC1123 DNS_LABEL). The resulting string must only consist of
    alphanumeric (lower-case a-z, and 0-9) and not exceed 63 characters. It's permitted to have '-' character as long
    as it's not in the first or last positions.

    :param Text value:
    :rtype: Text
    """
    res = ""
    MAX = 63
    HASH_LEN = 10
    if len(value) >= MAX:
        h = _sha224(value.encode("utf-8")).hexdigest()[:HASH_LEN]
        value = "{}-{}".format(h, value[-(MAX - HASH_LEN - 1) :])
    for ch in value:
        if ch == "_" or ch == "-" or ch == ".":
            # Convert '_' to '-' unless it's the first character, in which case we drop it.
            if res != "" and len(res) < 62:
                res += "-"
        elif not ch.isalnum():
            # Trim non-alphanumeric letters.
            pass
        elif ch.islower() or ch.isdigit():
            # Character is already compliant, just append it.
            res += ch
        else:
            # Character is upper-case. Add a '-' before it for better readability.
            if res != "" and res[-1] != "-" and len(res) < 62:
                res += "-"
            res += ch.lower()

    if len(res) > 0 and res[-1] == "-":
        res = res[: len(res) - 1]

    return res


def _get_container_definition(
    image: str,
    command: List[str],
    args: List[str],
    data_loading_config: Optional[_task_models.DataLoadingConfig] = None,
    storage_request: Optional[str] = None,
    ephemeral_storage_request: Optional[str] = None,
    cpu_request: Optional[str] = None,
    gpu_request: Optional[str] = None,
    memory_request: Optional[str] = None,
    storage_limit: Optional[str] = None,
    ephemeral_storage_limit: Optional[str] = None,
    cpu_limit: Optional[str] = None,
    gpu_limit: Optional[str] = None,
    memory_limit: Optional[str] = None,
    environment: Optional[Dict[str, str]] = None,
) -> _task_models.Container:
    storage_limit = storage_limit
    storage_request = storage_request
    ephemeral_storage_limit = ephemeral_storage_limit
    ephemeral_storage_request = ephemeral_storage_request
    cpu_limit = cpu_limit
    cpu_request = cpu_request
    gpu_limit = gpu_limit
    gpu_request = gpu_request
    memory_limit = memory_limit
    memory_request = memory_request

    requests = []
    if storage_request:
        requests.append(
            _task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.STORAGE, storage_request)
        )
    if ephemeral_storage_request:
        requests.append(
            _task_models.Resources.ResourceEntry(
                _task_models.Resources.ResourceName.EPHEMERAL_STORAGE, ephemeral_storage_request
            )
        )
    if cpu_request:
        requests.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.CPU, cpu_request))
    if gpu_request:
        requests.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.GPU, gpu_request))
    if memory_request:
        requests.append(
            _task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.MEMORY, memory_request)
        )

    limits = []
    if storage_limit:
        limits.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.STORAGE, storage_limit))
    if ephemeral_storage_limit:
        limits.append(
            _task_models.Resources.ResourceEntry(
                _task_models.Resources.ResourceName.EPHEMERAL_STORAGE, ephemeral_storage_limit
            )
        )
    if cpu_limit:
        limits.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.CPU, cpu_limit))
    if gpu_limit:
        limits.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.GPU, gpu_limit))
    if memory_limit:
        limits.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.MEMORY, memory_limit))

    if environment is None:
        environment = {}

    return _task_models.Container(
        image=image,
        command=command,
        args=args,
        resources=_task_models.Resources(limits=limits, requests=requests),
        env=environment,
        config={},
        data_loading_config=data_loading_config,
    )


def load_proto_from_file(pb2_type, path):
    with open(path, "rb") as reader:
        out = pb2_type()
        out.ParseFromString(reader.read())
        return out


def write_proto_to_file(proto, path):
    Path(_os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as writer:
        writer.write(proto.SerializeToString())


class Directory(object):
    def __init__(self, path):
        """
        :param Text path: local path of directory
        """
        self._name = path

    @property
    def name(self):
        """
        :rtype: Text
        """
        return self._name

    def list_dir(self):
        """
        The list of absolute filepaths for all immediate sub-paths
        :rtype: list[Text]
        """
        return [_os.path.join(self.name, f) for f in _os.listdir(self.name)]

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class AutoDeletingTempDir(Directory):
    """
    Creates a posix safe tempdir which is auto deleted once out of scope
    """

    def __init__(self, working_dir_prefix=None, tmp_dir=None, cleanup=True):
        """
        :param Text working_dir_prefix: A prefix to help identify temporary directories
        :param Text tmp_dir: Path to desired temporary directory
        :param bool cleanup: Whether the directory should be cleaned up upon exit
        """
        self._tmp_dir = tmp_dir
        self._working_dir_prefix = (working_dir_prefix + "_") if working_dir_prefix else ""
        self._cleanup = cleanup
        super(AutoDeletingTempDir, self).__init__(None)

    def __enter__(self):
        self._name = _tempfile.mkdtemp(dir=self._tmp_dir, prefix=self._working_dir_prefix)
        return self

    def get_named_tempfile(self, name):
        return _os.path.join(self.name, name)

    def _cleanup_dir(self):
        if self.name and self._cleanup:
            if _os.path.exists(self.name):
                _shutil.rmtree(self.name)
            self._name = None

    def force_cleanup(self):
        self._cleanup_dir()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup_dir()

    def __repr__(self):
        return "Auto-Deleting Tmp Directory @ {}".format(self.name)

    def __str__(self):
        return self.__repr__()


class PerformanceTimer(object):
    def __init__(self, context_statement):
        """
        :param Text context_statement: the statement to log
        """
        self._context_statement = context_statement
        self._start_wall_time = None
        self._start_process_time = None

    def __enter__(self):
        logger.info("Entering timed context: {}".format(self._context_statement))
        self._start_wall_time = _time.perf_counter()
        self._start_process_time = _time.process_time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_wall_time = _time.perf_counter()
        end_process_time = _time.process_time()
        logger.info(
            "Exiting timed context: {} [Wall Time: {}s, Process Time: {}s]".format(
                self._context_statement,
                end_wall_time - self._start_wall_time,
                end_process_time - self._start_process_time,
            )
        )
