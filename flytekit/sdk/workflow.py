from __future__ import absolute_import
from flytekit.common import workflow as _common_workflow, promise as _promise
from flytekit.common.types import helpers as _type_helpers
import six as _six


class Input(_promise.Input):
    """
    This object should be used to specify inputs. It can be used in conjunction with
    :py:meth:`flytekit.common.workflow.workflow` and :py:meth:`flytekit.common.workflow.workflow_class`
    """

    def __init__(self, sdk_type, help=None, **kwargs):
        """
        :param flytekit.common.types.base_sdk_types.FlyteSdkType sdk_type: This is the SDK type necessary to create an
            input to this workflow.
        :param Text help: An optional help string to describe the input to users.
        :param bool required: If set, default must be None
        :param T default: If this is not a required input, the value will default to this value.  Specify as a kwarg.
        """
        super(Input, self).__init__('', _type_helpers.python_std_to_sdk_type(sdk_type), help=help, **kwargs)


class Output(_common_workflow.Output):
    """
    This object should be used to specify outputs. It can be used in conjunction with
    :py:meth:`flytekit.common.workflow.workflow` and :py:meth:`flytekit.common.workflow.workflow_class`
    """

    def __init__(self, value, sdk_type=None, help=None):
        """
        :param T value:
        :param flytekit.common.types.base_sdk_types.FlyteSdkType sdk_type: If specified, the value provided must
            match this type exactly.  If not provided, the SDK will attempt to infer the type.  It is recommended
            this value be provided as the SDK might not always be able to infer the correct type.
        """
        super(Output, self).__init__(
            '',
            value,
            sdk_type=_type_helpers.python_std_to_sdk_type(sdk_type) if sdk_type else None,
            help=help
        )


def workflow_class(_workflow_metaclass=None, cls=None):
    """
    This is a decorator for wrapping class definitions into workflows.

     .. code-block:: python

        @workflow_class
        class MyWorkflow(object):
            a = Input(Types.Integer, default=100, help="Tell me something")
            b = Input(Types.Float, required=True)
            first_task = my_task(a=a)
            second_task = my_other_task(b=b, c=first_task.outputs.c)
            d = Output(node2.outputs.d)


    :param T _workflow_metaclass:  Do NOT specify this parameter directly.  This is the class that is being
        wrapped by this decorator.
    :param cls: This is the class that will be instantiated from the inputs, outputs, and nodes. This will be used
        by users extending the base Flyte programming model. If set, it must be a subclass of
        :py:class:`flytekit.common.workflow.SdkWorkflow`.
    :rtype: flytekit.common.workflow.SdkWorkflow
    """

    def wrapper(metaclass):
        wf = _common_workflow.build_sdk_workflow_from_metaclass(metaclass, cls=cls)
        return wf

    if _workflow_metaclass is not None:
        return wrapper(_workflow_metaclass)
    return wrapper


def workflow(nodes, inputs=None, outputs=None, cls=None):
    """
    This function provides a user-friendly interface for authoring workflows.

     .. code-block:: python

        input_a = Input(Types.Integer, default=100, help="Tell me something")
        input_b = Input(Types.Float, required=True)

        node1 = my_task(a=input_a)
        node2 = my_other_task(b=input_b, c=node1.outputs.c)

        MyWorkflow = workflow(
            workflow_id='my_workflow_id',
            inputs={
                'a': input_a,
                'b': input_b
            },
            outputs={
                'd': Output(node2.outputs.d, sdk_type=Types.Integer, help='This is an integer output')
            },
            nodes=[
                node1,
                node2
            ]
        )

    :param dict[Text,flytekit.common.nodes.SdkNode] nodes: A list of nodes to put inside the workflow.
    :param dict[Text,Input] inputs: [Optional] A dictionary of input descriptors for the workflow.
    :param dict[Text,Output] outputs: [Optional] A dictionary of output descriptors for a workflow.
    :param T cls: This is the class that will be instantiated from the inputs, outputs, and nodes. This will be used
        by users extending the base Flyte programming model. If set, it must be a subclass of
        :py:class:`flytekit.common.workflow.SdkWorkflow`.
    :rtype: flytekit.common.workflow.SdkWorkflow
    """
    wf = (cls or _common_workflow.SdkWorkflow)(
        inputs=[v.rename_and_return_reference(k) for k, v in sorted(_six.iteritems(inputs or {}))],
        outputs=[v.rename_and_return_reference(k) for k, v in sorted(_six.iteritems(outputs or {}))],
        nodes=[v.assign_id_and_return(k) for k, v in sorted(_six.iteritems(nodes))]
    )
    return wf
