from __future__ import absolute_import

import pytest

from flytekit.common import constants
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import base_sdk_types, containers, primitives
from flytekit.sdk.tasks import inputs, outputs, python_task
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import Input, Output, workflow, workflow_class


def test_input():
    i = Input(primitives.Integer, help="blah", default=None)
    assert i.name == ""
    assert i.sdk_default is None
    assert i.default == base_sdk_types.Void()
    assert i.sdk_required is False
    assert i.required is None
    assert i.help == "blah"
    assert i.var.description == "blah"
    assert i.sdk_type == primitives.Integer

    i = i.rename_and_return_reference("new_name")
    assert i.name == "new_name"
    assert i.sdk_default is None
    assert i.default == base_sdk_types.Void()
    assert i.sdk_required is False
    assert i.required is None
    assert i.help == "blah"
    assert i.var.description == "blah"
    assert i.sdk_type == primitives.Integer

    i = Input(primitives.Integer, default=1)
    assert i.name == ""
    assert i.sdk_default == 1
    assert i.default == primitives.Integer(1)
    assert i.sdk_required is False
    assert i.required is None
    assert i.help is None
    assert i.var.description == ""
    assert i.sdk_type == primitives.Integer

    i = i.rename_and_return_reference("new_name")
    assert i.name == "new_name"
    assert i.sdk_default == 1
    assert i.default == primitives.Integer(1)
    assert i.sdk_required is False
    assert i.required is None
    assert i.help is None
    assert i.var.description == ""
    assert i.sdk_type == primitives.Integer

    with pytest.raises(_user_exceptions.FlyteAssertion):
        Input(primitives.Integer, required=True, default=1)

    i = Input([primitives.Integer], default=[1, 2])
    assert i.name == ""
    assert i.sdk_default == [1, 2]
    assert i.default == containers.List(primitives.Integer)([primitives.Integer(1), primitives.Integer(2)])
    assert i.sdk_required is False
    assert i.required is None
    assert i.help is None
    assert i.var.description == ""
    assert i.sdk_type == containers.List(primitives.Integer)

    i = i.rename_and_return_reference("new_name")
    assert i.name == "new_name"
    assert i.sdk_default == [1, 2]
    assert i.default == containers.List(primitives.Integer)([primitives.Integer(1), primitives.Integer(2)])
    assert i.sdk_required is False
    assert i.required is None
    assert i.help is None
    assert i.var.description == ""
    assert i.sdk_type == containers.List(primitives.Integer)


def test_output():
    o = Output(1, sdk_type=primitives.Integer, help="blah")
    assert o.name == ""
    assert o.var.description == "blah"
    assert o.var.type == primitives.Integer.to_flyte_literal_type()
    assert o.binding_data.scalar.primitive.integer == 1

    o = o.rename_and_return_reference("new_name")
    assert o.name == "new_name"
    assert o.var.description == "blah"
    assert o.var.type == primitives.Integer.to_flyte_literal_type()
    assert o.binding_data.scalar.primitive.integer == 1


def _get_node_by_id(wf, nid):
    for n in wf.nodes:
        if n.id == nid:
            return n
    assert False


def test_workflow_no_node_dependencies_or_outputs():
    @inputs(a=Types.Integer)
    @outputs(b=Types.Integer)
    @python_task
    def my_task(wf_params, a, b):
        b.set(a + 1)

    i1 = Input(Types.Integer)
    i2 = Input(Types.Integer, default=5, help="Not required.")

    input_dict = {"input_1": i1, "input_2": i2}

    nodes = {
        "a": my_task(a=input_dict["input_1"]),
        "b": my_task(a=input_dict["input_2"]),
        "c": my_task(a=100),
    }

    w = workflow(inputs=input_dict, outputs={}, nodes=nodes)

    assert w.interface.inputs["input_1"].type == Types.Integer.to_flyte_literal_type()
    assert w.interface.inputs["input_2"].type == Types.Integer.to_flyte_literal_type()
    assert _get_node_by_id(w, "a").inputs[0].var == "a"
    assert _get_node_by_id(w, "a").inputs[0].binding.promise.node_id == constants.GLOBAL_INPUT_NODE_ID
    assert _get_node_by_id(w, "a").inputs[0].binding.promise.var == "input_1"
    assert _get_node_by_id(w, "b").inputs[0].binding.promise.node_id == constants.GLOBAL_INPUT_NODE_ID
    assert _get_node_by_id(w, "b").inputs[0].binding.promise.var == "input_2"
    assert _get_node_by_id(w, "c").inputs[0].binding.scalar.primitive.integer == 100


def test_workflow_metaclass_no_node_dependencies_or_outputs():
    @inputs(a=Types.Integer)
    @outputs(b=Types.Integer)
    @python_task
    def my_task(wf_params, a, b):
        b.set(a + 1)

    @workflow_class
    class sup(object):
        input_1 = Input(Types.Integer)
        input_2 = Input(Types.Integer, default=5, help="Not required.")

        a = my_task(a=input_1)
        b = my_task(a=input_2)
        c = my_task(a=100)

    assert sup.interface.inputs["input_1"].type == Types.Integer.to_flyte_literal_type()
    assert sup.interface.inputs["input_2"].type == Types.Integer.to_flyte_literal_type()
    assert _get_node_by_id(sup, "a").inputs[0].var == "a"
    assert _get_node_by_id(sup, "a").inputs[0].binding.promise.node_id == constants.GLOBAL_INPUT_NODE_ID
    assert _get_node_by_id(sup, "a").inputs[0].binding.promise.var == "input_1"
    assert _get_node_by_id(sup, "b").inputs[0].binding.promise.node_id == constants.GLOBAL_INPUT_NODE_ID
    assert _get_node_by_id(sup, "b").inputs[0].binding.promise.var == "input_2"
    assert _get_node_by_id(sup, "c").inputs[0].binding.scalar.primitive.integer == 100
