from __future__ import absolute_import

from google.protobuf.json_format import MessageToDict as _MessageToDict
import logging as _logging

from flytekit.common import constants as _constants
from flytekit.common.tasks import task as _base_task
from flytekit.models import (
    interface as _interface_model
)
from flytekit.models import interface as _interface, \
    literals as _literals, types as _types, task as _task_model

class SdkPrestoTask(_base_task.SdkTask):
    """
    This class includes the logic for building a task that executes as a presto task.
    """

    def __init__(
            self,
            hive_job,
            discoverable,
            discovery_version,
            retries,
            timeout,
            cluster_label,
            tags,
            environment
    ):
        """
        TODO: The first param needs to become a Presto model ofc.
        :param _qubole.QuboleHiveJob hive_job: Hive job spec
        :param bool discoverable:
        :param Text discovery_version: string describing the version for task discovery purposes
        :param int retries: Number of retries to attempt
        :param datetime.timedelta timeout:
        :param Text cluster_label:
        :param list[Text] tags:
        :param dict[Text, Text] environment:
        :param TaskMetadata metadata: This contains information needed at runtime to determine behavior such as
            whether or not outputs are discoverable, timeouts, and retries.
        """
        metadata = _task_model.TaskMetadata(
            discoverable,
            # This needs to have the proper version reflected in it
            _task_model.RuntimeMetadata(_task_model.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
            timeout,
            _literals.RetryStrategy(retries),
            discovery_version,
            "This is deprecated!"
        )

        super(SdkPrestoTask, self).__init__(
            _constants.SdkTaskType.PRESTO_TASK,
            metadata,
            # the typed interface should not have any inputs, but I think maybe it should take an output schema type
            # which means you'll need to pipe it in through the constructor as well.
            _interface_model.TypedInterface({}, {}),
            _MessageToDict(hive_job),
        )