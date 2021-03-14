from collections import OrderedDict

from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.node_creation import create_node
from flytekit.core.task import task
from flytekit.core.workflow import workflow, Workflow, WorkflowTwo


def test_wf2():
    @task
    def t1(a: str) -> str:
        return a + " world"

    wb = WorkflowTwo(name="my.workflow")
    wb.add_workflow_input("in1", str)
    node = wb.add_entity(t1, a=wb.inputs["in1"])
    wb.add_workflow_output("from_n0t1", str, node.o0)
    print(node)
    print(wb)

    wb(in1="hello")