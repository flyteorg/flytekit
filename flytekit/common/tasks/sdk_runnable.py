from __future__ import annotations

import logging as _logging
import os
import pathlib
import typing
from dataclasses import dataclass
from datetime import datetime

from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common import utils as _common_utils
from flytekit.configuration import internal as _internal_config
from flytekit.configuration import sdk as _sdk_config
from flytekit.common.interfaces.stats import taggable
from flytekit.models import task as _task_models


# TODO: Clean up working dir name
class ExecutionParameters(object):
    """
    This is the parameter object that will be provided as the first parameter for every execution of any @*_task
    decorated function.
    """

    @dataclass(init=False)
    class Builder(object):
        stats: taggable.TaggableStats
        execution_date: datetime
        logging: _logging
        execution_id: str
        attrs: typing.Dict[str, typing.Any]
        working_dir: typing.Union[os.PathLike, _common_utils.AutoDeletingTempDir]

        def __init__(self, current: ExecutionParameters = None):
            self.stats = current.stats if current else None
            self.execution_date = current.execution_date if current else None
            self.working_dir = current.working_directory if current else None
            self.execution_id = current.execution_id if current else None
            self.logging = current.logging if current else None
            self.attrs = current._attrs if current else {}

        def add_attr(self, key: str, v: typing.Any) -> ExecutionParameters.Builder:
            self.attrs[key] = v
            return self

        def build(self) -> ExecutionParameters:
            if not isinstance(self.working_dir, _common_utils.AutoDeletingTempDir):
                pathlib.Path(self.working_dir).mkdir(parents=True, exist_ok=True)
            return ExecutionParameters(
                execution_date=self.execution_date,
                stats=self.stats,
                tmp_dir=self.working_dir,
                execution_id=self.execution_id,
                logging=self.logging,
                **self.attrs,
            )

    @staticmethod
    def new_builder(current: ExecutionParameters = None) -> Builder:
        return ExecutionParameters.Builder(current=current)

    def builder(self) -> Builder:
        return ExecutionParameters.Builder(current=self)

    def __init__(self, execution_date, tmp_dir, stats, execution_id, logging, **kwargs):
        """
        Args:
            execution_date: Date when the execution is running
            tmp_dir: temporary directory for the execution
            stats: handle to emit stats
            execution_id: Identifier for the xecution
            logging: handle to logging
        """
        self._stats = stats
        self._execution_date = execution_date
        self._working_directory = tmp_dir
        self._execution_id = execution_id
        self._logging = logging
        # AutoDeletingTempDir's should be used with a with block, which creates upon entry
        self._attrs = kwargs

    @property
    def stats(self) -> taggable.TaggableStats:
        """
        A handle to a special statsd object that provides usefully tagged stats.
        TODO: Usage examples and better comments
        """
        return self._stats

    @property
    def logging(self) -> _logging:
        """
        A handle to a useful logging object.
        TODO: Usage examples
        """
        return self._logging

    @property
    def working_directory(self) -> _common_utils.AutoDeletingTempDir:
        """
        A handle to a special working directory for easily producing temporary files.

        TODO: Usage examples
        TODO: This does not always return a AutoDeletingTempDir
        """
        return self._working_directory

    @property
    def execution_date(self) -> datetime:
        """
        This is a datetime representing the time at which a workflow was started.  This is consistent across all tasks
        executed in a workflow or sub-workflow.

        .. note::

            Do NOT use this execution_date to drive any production logic.  It might be useful as a tag for data to help
            in debugging.
        """
        return self._execution_date

    @property
    def execution_id(self) -> str:
        """
        This is the identifier of the workflow execution within the underlying engine.  It will be consistent across all
        task executions in a workflow or sub-workflow execution.

        .. note::

            Do NOT use this execution_id to drive any production logic.  This execution ID should only be used as a tag
            on output data to link back to the workflow run that created it.
        """
        return self._execution_id

    def __getattr__(self, attr_name: str) -> typing.Any:
        """
        This houses certain task specific context. For example in Spark, it houses the SparkSession, etc
        """
        attr_name = attr_name.upper()
        if self._attrs and attr_name in self._attrs:
            return self._attrs[attr_name]
        raise AssertionError(f"{attr_name} not available as a parameter in Flyte context - are you in right task-type?")

    def has_attr(self, attr_name: str) -> bool:
        attr_name = attr_name.upper()
        if self._attrs and attr_name in self._attrs:
            return True
        return False


class SdkRunnableContainer(_task_models.Container, metaclass=_sdk_bases.ExtendedSdkType):
    """
    This is not necessarily a local-only Container object. So long as configuration is present, you can use this object
    """

    def __init__(
        self, command, args, resources, env, config,
    ):
        super(SdkRunnableContainer, self).__init__("", command, args, resources, env or {}, config)

    @property
    def args(self):
        """
        :rtype: list[Text]
        """
        return _sdk_config.SDK_PYTHON_VENV.get() + self._args

    @property
    def image(self):
        """
        :rtype: Text
        """
        return _internal_config.IMAGE.get()

    @property
    def env(self):
        """
        :rtype: dict[Text,Text]
        """
        env = super(SdkRunnableContainer, self).env.copy()
        env.update(
            {
                _internal_config.CONFIGURATION_PATH.env_var: _internal_config.CONFIGURATION_PATH.get(),
                _internal_config.IMAGE.env_var: _internal_config.IMAGE.get(),
                # TODO: Phase out the below.  Propeller will set these and these are not SDK specific
                _internal_config.PROJECT.env_var: _internal_config.PROJECT.get(),
                _internal_config.DOMAIN.env_var: _internal_config.DOMAIN.get(),
                _internal_config.NAME.env_var: _internal_config.NAME.get(),
                _internal_config.VERSION.env_var: _internal_config.VERSION.get(),
            }
        )
        return env

    @classmethod
    def get_resources(
        cls,
        storage_request=None,
        cpu_request=None,
        gpu_request=None,
        memory_request=None,
        storage_limit=None,
        cpu_limit=None,
        gpu_limit=None,
        memory_limit=None,
    ):
        """
        :param Text storage_request:
        :param Text cpu_request:
        :param Text gpu_request:
        :param Text memory_request:
        :param Text storage_limit:
        :param Text cpu_limit:
        :param Text gpu_limit:
        :param Text memory_limit:
        """
        requests = []
        if storage_request:
            requests.append(
                _task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.STORAGE, storage_request)
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
            limits.append(
                _task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.STORAGE, storage_limit)
            )
        if cpu_limit:
            limits.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.CPU, cpu_limit))
        if gpu_limit:
            limits.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.GPU, gpu_limit))
        if memory_limit:
            limits.append(
                _task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.MEMORY, memory_limit)
            )

        return _task_models.Resources(limits=limits, requests=requests)
