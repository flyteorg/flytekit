from __future__ import absolute_import

from google.protobuf.json_format import MessageToDict as _MessageToDict
import logging as _logging
from flytekit.sdk.types import Types


from flytekit.common import constants as _constants
from flytekit.common.tasks import task as _base_task
from flytekit.models import (
    interface as _interface_model
)
from flytekit.models import interface as _interface, \
    literals as _literals, types as _types, task as _task_model

from flytekit.common import interface

# from flyteidl.plugins.presto_pb2 import PrestoQuery
from flytekit.models.presto import PrestoQuery

class SdkPrestoTask(_base_task.SdkTask):
    """
    This class includes the logic for building a task that executes as a presto task.
    """

    def __init__(
            self,
            query,
            typed_schema,
            discoverable,
            discovery_version,
            retries,
            timeout,
            routing_group,
            catalog,
            schema,
    ):
        """
        TODO: The first param needs to become a Presto model ofc.
        :param Text query: Presto query spec
        :param flytekit.common.types.schema.Schema typed_schema: Schema that represents that data queried from Presto
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

        presto_query = PrestoQuery(
            routing_group=routing_group,
            catalog=catalog,
            schema=schema,
            statement=query
        )

        literal_type = _types.LiteralType(
            schema=typed_schema
        )

        variable = _interface_model.Variable(
            type=literal_type,
            description="TODO"
        )


        var_map = _interface_model.VariableMap(
            variables={"schema": variable}
        )

        x = _interface_model.TypedInterface({}, var_map)
        y = interface.TypedInterface.promote_from_model(x)

        super(SdkPrestoTask, self).__init__(
            _constants.SdkTaskType.PRESTO_TASK,
            metadata,
            y,
            _MessageToDict(presto_query.to_flyte_idl()),
        )
