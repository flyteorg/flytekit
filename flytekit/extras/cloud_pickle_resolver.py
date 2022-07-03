import gzip
import os
import pickle
import tempfile
from typing import List

import cloudpickle

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import TaskResolverMixin
from flytekit.core.python_auto_container import PythonAutoContainerTask
from flytekit.core.tracker import TrackedInstance
from flytekit.loggers import remote_logger


class CloudPickleResolver(TrackedInstance, TaskResolverMixin):
    """
    Pickles the task (using cloudpickle) and ships it off for Data Persistence (S3, etc.)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def name(self) -> str:
        return "CloudPickleResolver"

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        uri = loader_args[0]

        # TODO: get gzipped file from data persistence using URI from loader_args
        filename = ...

        with gzip.open(filename, mode="rb") as fg:
            t_bytes = fg.read()

        remote_logger.info(f"Task of size {len(t_bytes)} bytes downloaded from {uri}")

        return pickle.loads(t_bytes)

    def loader_args(self, settings: SerializationSettings, t: PythonAutoContainerTask) -> List[str]:
        with tempfile.TemporaryDirectory() as tmp_dir:
            t_bytes = cloudpickle.dumps(t)
            t_fname = os.path.join(tmp_dir, "task.pickle")
            with open(t_fname, "wb") as fh:
                fh.write(t_bytes)
            t_fname_gzipped = os.path.join(tmp_dir, "task.tgz")
            with gzip.GzipFile(filename=t_fname_gzipped, mode="wb", mtime=0) as gzipped:
                with open(t_fname, "rb") as fd:
                    gzipped.write(fd.read())

            # TODO: Upload gzipped file for data persistence and get its URI
            uri = ...

            remote_logger.info(
                f"Task {t.name} zipped, pickled and persisted to {uri} from temporary file {t_fname_gzipped}"
            )

            return [uri]

    def get_all_tasks(self) -> List[PythonAutoContainerTask]:
        raise NotImplementedError()


default_cloudpickle_resolver = CloudPickleResolver()
