import datetime as _datetime

import six as _six
from google.protobuf.json_format import MessageToDict as _MessageToDict

from flytekit import __version__
from flytekit.common import constants as _constants
from flytekit.common import interface as _interface
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks import task as _base_task
from flytekit.common.types import helpers as _type_helpers
from flytekit.models import interface as _interface_model
from flytekit.models import literals as _literals
from flytekit.models import presto as _presto_models
from flytekit.models import task as _task_model
from flytekit.models import types as _types


class SdkPrestoTask(_base_task.SdkTask):
    """
    This class includes the logic for building a task that executes as a Presto task.
    """

    def __init__(
        self,
        statement,
        output_schema,
        routing_group=None,
        catalog=None,
        schema=None,
        task_inputs=None,
        interruptible=False,
        discoverable=False,
        discovery_version=None,
        retries=1,
        timeout=None,
        deprecated=None,
        cache_serializable=False,
    ):
        """
        :param Text statement: Presto query specification
        :param flytekit.common.types.schema.Schema output_schema: Schema that represents that data queried from Presto
        :param Text routing_group: The routing group that a Presto query should be sent to for the given environment
        :param Text catalog: The catalog to set for the given Presto query
        :param Text schema: The schema to set for the given Presto query
        :param dict[Text,flytekit.common.types.base_sdk_types.FlyteSdkType] task_inputs: Optional inputs to the Presto task
        :param bool discoverable:
        :param Text discovery_version: String describing the version for task discovery purposes
        :param int retries: Number of retries to attempt
        :param datetime.timedelta timeout:
        :param Text deprecated: This string can be used to mark the task as deprecated.  Consumers of the task will
            receive deprecation warnings.
        :param bool cache_serializable:
        """

        # Set as class fields which are used down below to configure implicit
        # parameters
        self._routing_group = routing_group or ""
        self._catalog = catalog or ""
        self._schema = schema or ""

        metadata = _task_model.TaskMetadata(
            discoverable,
            # This needs to have the proper version reflected in it
            _task_model.RuntimeMetadata(_task_model.RuntimeMetadata.RuntimeType.FLYTE_SDK, __version__, "python"),
            timeout or _datetime.timedelta(seconds=0),
            _literals.RetryStrategy(retries),
            interruptible,
            discovery_version,
            deprecated,
            cache_serializable,
        )

        presto_query = _presto_models.PrestoQuery(
            routing_group=routing_group or "",
            catalog=catalog or "",
            schema=schema or "",
            statement=statement,
        )

        # Here we set the routing_group, catalog, and schema as implicit
        # parameters for caching purposes
        i = _interface.TypedInterface(
            {
                "__implicit_routing_group": _interface_model.Variable(
                    type=_types.LiteralType(simple=_types.SimpleType.STRING),
                    description="The routing group set as an implicit input",
                ),
                "__implicit_catalog": _interface_model.Variable(
                    type=_types.LiteralType(simple=_types.SimpleType.STRING),
                    description="The catalog set as an implicit input",
                ),
                "__implicit_schema": _interface_model.Variable(
                    type=_types.LiteralType(simple=_types.SimpleType.STRING),
                    description="The schema set as an implicit input",
                ),
            },
            {
                # Set the schema for the Presto query as an output
                "results": _interface_model.Variable(
                    type=_types.LiteralType(schema=output_schema.schema_type),
                    description="The schema for the Presto query",
                )
            },
        )

        super(SdkPrestoTask, self).__init__(
            _constants.SdkTaskType.PRESTO_TASK,
            metadata,
            i,
            _MessageToDict(presto_query.to_flyte_idl()),
        )

        # Set user provided inputs
        task_inputs(self)

    def _add_implicit_inputs(self, inputs):
        """
        :param dict[Text,Any] inputs:
        :param inputs:
        :return:
        """
        inputs["__implicit_routing_group"] = self.routing_group
        inputs["__implicit_catalog"] = self.catalog
        inputs["__implicit_schema"] = self.schema
        return inputs

    # Override method in order to set the implicit inputs
    def __call__(self, *args, **kwargs):
        kwargs = self._add_implicit_inputs(kwargs)

        return super(SdkPrestoTask, self).__call__(*args, **kwargs)

    # Override method in order to set the implicit inputs
    def _python_std_input_map_to_literal_map(self, inputs):
        """
        :param dict[Text,Any] inputs: A dictionary of Python standard inputs that will be type-checked and compiled
            to a LiteralMap
        :rtype: flytekit.models.literals.LiteralMap
        """
        inputs = self._add_implicit_inputs(inputs)
        return _type_helpers.pack_python_std_map_to_literal_map(
            inputs,
            {k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in _six.iteritems(self.interface.inputs)},
        )

    @_exception_scopes.system_entry_point
    def add_inputs(self, inputs):
        """
        Adds the inputs to this task.  This can be called multiple times, but it will fail if an input with a given
        name is added more than once, a name collides with an output, or if the name doesn't exist as an arg name in
        the wrapped function.
        :param dict[Text, flytekit.models.interface.Variable] inputs: names and variables
        """
        self._validate_inputs(inputs)
        self.interface.inputs.update(inputs)

    @property
    def routing_group(self):
        """
        The routing group that a Presto query should be sent to for the given environment
        :rtype: Text
        """
        return self._routing_group

    @property
    def catalog(self):
        """
        The catalog to set for the given Presto query
        :rtype: Text
        """
        return self._catalog

    @property
    def schema(self):
        """
        The schema to set for the given Presto query
        :rtype: Text
        """
        return self._schema
