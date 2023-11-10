import io
import sys
import typing
from collections import OrderedDict
from datetime import timedelta
from io import StringIO

import pytest
from mock import patch

import flytekit
from flytekit import dynamic, map_task
from flytekit.configuration import Image, ImageConfig
from flytekit.core.condition import conditional
from flytekit.core.gate import wait_for_input
from flytekit.core.node_creation import create_node
from flytekit.core.task import TaskMetadata, task
from flytekit.core.workflow import workflow
from flytekit.tools.translator import get_serializable

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)

"""
Test for simple tasks/subworkflow chaining
"""


def test_task_local_chain():
    @task
    def task_a():
        print("a")

    @task
    def task_b():
        print("b")

    @task
    def task_c():
        print("c")

    @workflow()
    def my_wf():
        a = task_a()
        b = task_b()
        c = task_c()

        c >> b >> a

    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    my_wf()
    sys.stdout = sys.__stdout__
    assert capturedOutput.getvalue() == "c\nb\na\n"


def test_subworkflow_local_chain():
    @task
    def task_a():
        print("a")

    @task
    def task_b():
        print("b")

    @task
    def task_c():
        print("c")

    @workflow
    def sub_wf():
        t2 = task_b()
        t3 = task_c()

        t3 >> t2

    @workflow()
    def my_wf():
        sf = sub_wf()
        t3 = task_a()
        t2 = task_b()
        t1 = task_c()

        t1 >> t2 >> t3 >> sf

    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    my_wf()
    sys.stdout = sys.__stdout__
    assert capturedOutput.getvalue() == "c\nb\na\nc\nb\n"


def test_task_pass_int_local_chain():
    @task
    def task_five() -> int:
        print("5")
        return 5

    @task
    def task_six() -> int:
        print("6")
        return 6

    @task
    def task_add(a: int, b: int) -> int:
        print(a + b)
        return a + b

    @workflow()
    def my_wf() -> int:
        five = task_five()
        six = task_six()
        add = task_add(a=five, b=six)
        six >> five >> add
        return add

    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    result = my_wf()
    sys.stdout = sys.__stdout__
    assert capturedOutput.getvalue() == "6\n5\n11\n"
    assert result == 11


def test_wf_nested_comp():
    @task
    def t1(a: int) -> int:
        print("t1", a)
        a = a + 5
        return a

    @workflow()
    def outer() -> typing.Tuple[int, int]:
        # You should not do this. This is just here for testing.
        @workflow
        def wf2() -> int:
            print("wf2")
            return t1(a=5)

        ft = t1(a=3)
        fwf = wf2()
        fwf >> ft
        return ft, fwf

    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    _ = outer()
    sys.stdout = sys.__stdout__
    assert capturedOutput.getvalue() == "wf2\nt1 5\nt1 3\n"

    assert (8, 10) == outer()
    entity_mapping = OrderedDict()

    model_wf = get_serializable(entity_mapping, serialization_settings, outer)

    assert len(model_wf.template.interface.outputs) == 2
    assert len(model_wf.template.nodes) == 2
    assert model_wf.template.nodes[1].workflow_node is not None

    sub_wf = model_wf.sub_workflows[0]
    assert len(sub_wf.nodes) == 1
    assert sub_wf.nodes[0].id == "n0"
    assert sub_wf.nodes[0].task_node.reference_id.name == "tests.flytekit.unit.core.test_workflows_local_chain.t1"


"""
Test for failing chaining
"""


def test_cycle_fail():
    @task
    def five() -> int:
        print("five")
        return 5

    @task
    def t1(a: int) -> int:
        print("t1", a)
        a = a + 5
        return a

    @task
    def t2(a: int) -> int:
        print("t2", a)
        a = a + 5
        return a

    @workflow
    def wf():
        a = five()
        b = t1(a=a)
        c = t2(a=b)
        c >> b >> a  # wrong sequence will cause cycle

    @workflow
    def wf_multiple_call():
        a = five()
        b = t1(a=a)
        c = t2(a=b)

        a >> a >> b >> c

    # c >> b >> a is an invalid execute sequence
    with pytest.raises(Exception):
        wf()
    # a is called multiple times.
    with pytest.raises(Exception):
        wf_multiple_call()


"""
Test for conditional chaining
"""


def test_condition_local_chain():
    @task
    def square(n: float) -> float:
        print("square")
        return n * n

    @task
    def double(n: float) -> float:
        print("double")
        return 2 * n

    @workflow()
    def multiplier_3(my_input: float):
        a = (
            conditional("fractions")
            .if_((my_input >= 0) & (my_input < 1.0))
            .then(double(n=my_input))
            .else_()
            .then(square(n=my_input))
        )
        b = (
            conditional("fractions2")
            .if_((my_input >= 0) & (my_input < 1.0))
            .then(square(n=my_input))
            .else_()
            .then(double(n=my_input))
        )

        b >> a

    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    multiplier_3(my_input=0.5)  # call square first, then call double when input = 0.5
    sys.stdout = sys.__stdout__
    assert capturedOutput.getvalue() == "square\ndouble\n"


"""
Test for dynamic chaining
"""


def test_wf_dynamic_local_chain():
    @task
    def t1(a: int) -> int:
        print("t1")
        a = a + 2
        return a

    @dynamic
    def use_result(a: int) -> int:
        print("call use_result")
        if a > 6:
            return 5
        else:
            return 0

    @dynamic
    def use_result2(a: int) -> int:
        print("call use_result2")
        if a > 6:
            return 0
        else:
            return 5

    @task
    def t2(a: int) -> int:
        print("t2")
        return a + 3

    @workflow
    def wf():
        a1 = t1(a=7)
        a2 = t2(a=9)
        b1 = use_result(a=a1)
        b2 = use_result2(a=a1)
        a1 >> b2 >> b1 >> a2

    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    wf()
    sys.stdout = sys.__stdout__
    assert capturedOutput.getvalue() == "t1\ncall use_result2\ncall use_result\nt2\n"


def test_create_node_dynamic_local_local_chain():
    @task
    def task1(s: str) -> str:
        print("task1")
        return s

    @task
    def task2(s: str) -> str:
        print("task2")
        return s

    @dynamic
    def dynamic_wf() -> str:
        node_1 = create_node(task1, s="hello")
        node_2 = create_node(task2, s="world")
        node_2 >> node_1

        return node_1.o0

    @workflow
    def wf() -> str:
        return dynamic_wf()

    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    assert wf() == "hello"
    sys.stdout = sys.__stdout__
    assert capturedOutput.getvalue() == "task2\ntask1\n"


"""
Test for gate chaining.
"""


def test_dyn_signal_no_approve_local_chain():
    @task
    def t1(a: int) -> int:
        return a + 5

    @task
    def t2(a: int) -> int:
        return a + 6

    @dynamic
    def dyn(a: int) -> typing.Tuple[int, int]:
        x = t1(a=a)
        s1 = wait_for_input("my-signal-name", timeout=timedelta(hours=1), expected_type=bool)
        s2 = wait_for_input("my-signal-name-2", timeout=timedelta(hours=2), expected_type=int)
        z = t1(a=3)
        y = t2(a=s2)
        z >> x >> s2 >> s1

        return y, z

    @workflow
    def wf_dyn(a: int) -> typing.Tuple[int, int]:
        y, z = dyn(a=a)
        return y, z

    with patch("sys.stdin", StringIO("3\ny\n")) as stdin, patch("sys.stdout", new_callable=StringIO):
        capturedOutput = io.StringIO()
        sys.stdout = capturedOutput
        wf_dyn(a=5)
        sys.stdout = sys.__stdout__
        gate_message = "[Input Gate] Waiting for input @my-signal-name-2 of type <class 'int'>: "
        gate_message += "[Input Gate] Waiting for input @my-signal-name of type <class 'bool'>: "
        assert capturedOutput.getvalue() == gate_message
        assert stdin.read() == ""  # all input consumed


"""
Test map task chaining
"""


def test_map_task_chaining():
    @task
    def complex_task(a: int) -> str:
        print("t1")
        b = a + 2
        return str(b)

    @task
    def complex_task2(a: int) -> str:
        print("t2")
        b = a + 5
        return str(b)

    maptask = map_task(complex_task, metadata=TaskMetadata(retries=1))
    maptask2 = map_task(complex_task2, metadata=TaskMetadata(retries=1))

    @workflow
    def w1(a: typing.List[int]) -> typing.List[str]:
        t1 = maptask(a=a)
        t2 = maptask2(a=a)
        t2 >> t1
        return t1

    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    res = w1(a=[1, 2, 3])
    sys.stdout = sys.__stdout__
    assert capturedOutput.getvalue() == "t2\nt2\nt2\nt1\nt1\nt1\n"
    assert res == ["3", "4", "5"]
