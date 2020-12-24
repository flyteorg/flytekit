from __future__ import annotations

import datetime
from typing import Any, List, Optional, Union

from flytekit.annotated import interface as flyte_interface
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.promise import Promise, binding_from_python_std, create_task_output
from flytekit.common import constants as _common_constants
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.nodes import SdkNode
from flytekit.common.promise import NodeOutput as _NodeOutput
from flytekit.common.utils import _dnsify
from flytekit.models import literals as _literal_models
from flytekit.models.core import workflow as _workflow_model
from flytekit.annotated.node import Node
from flytekit.annotated.base_task import PythonTask
from flytekit.annotated.launch_plan import LaunchPlan
from flytekit.annotated.workflow import Workflow


def run(entity: Union[PythonTask, LaunchPlan, Workflow], *args, **kwargs) -> Node:
    """
    This is the function you want to call if you need to specify dependencies between tasks that don't consume and/or
    don't produce outputs. For example, if you have t1() and t2(), both of which do not take in nor produce any
    outputs, how do you specify that t2 should run before t1?

        t2_node = flytekit.run(t2)
        t1_node = flytekit.run(t1)

        t2_node >> t1_node   # OR you can do,
        t1_node.depends_on(t2_node)

    This works for tasks that take inputs as well, say a ``t3(in1: int)``

        t3_node = flytekit.run(t3, in1=some_int)  # basically calling t3(in1=some_int)

    You can still use this method to handle setting certain overrides

        t3_node = flytekit.run(t3, in1=some_int).with_overrides(...)

    Outputs, if there are any, will be accessible. A `t4() -> (int, str)`

        t4_node = flytekit.run(t4)
        t5(in1=t4_node.o0)

    @workflow
    def wf():
        run(sub_wf)
        run(wf2)

    @dynamic
    def sub_wf():
        run(other_sub)
        run(task)
    """
    if len(args) > 0:
        raise _user_exceptions.FlyteAssertion(
            f"Only keyword args are supported to pass inputs to workflows and tasks."
            f"Aborting execution as detected {len(args)} positional args {args}"
        )

    if not isinstance(entity, PythonTask) and not isinstance(entity, Workflow) and not isinstance(entity, LaunchPlan):
        raise AssertionError("Should be but it's not")

    # This function is only called from inside workflows and dynamic tasks.
    # That means there are two scenarios we need to take care of, compilation and local workflow execution.

    # whatever happens, we need to call this thing
    #
    ctx = FlyteContext.current_context()
    # When compiling, calling the entity will create a node.
    if ctx.compilation_state is not None and ctx.compilation_state.mode == 1:
        outputs = entity(**kwargs)

        # What are the things that outputs can be?
        node = ctx.compilation_state.nodes[-1]

        return node
    else:
        raise Exception("fdsjaklfjasdk")


