import typing
from collections import OrderedDict
from datetime import timedelta
from io import StringIO

from mock import patch

import flytekit.configuration
from flytekit.configuration import Image, ImageConfig
from flytekit.core.gate import signal, approve
from flytekit.core.task import task
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


def test_basic_signal():
    @task
    def t1(a: int) -> int:
        return a + 5

    @task
    def t2(a: int) -> int:
        return a + 6

    @workflow
    def wf(a: int) -> typing.Tuple[int, int, int]:
        x = t1(a=a)
        s1 = signal("my-signal-name", timeout=timedelta(hours=1), expected_type=bool)
        s2 = signal("my-signal-name-2", timeout=timedelta(hours=2), expected_type=int)
        z = t1(a=5)
        y = t2(a=s2)
        q = t2(a=approve(y, "approvalfory", timeout=timedelta(hours=2)))
        x >> s1
        s1 >> z

        return y, z, q

    with patch("sys.stdin", StringIO("\n3\n")) as stdin, patch("sys.stdout", new_callable=StringIO):
        res = wf(a=5)
        assert res == (9, 10)
        assert stdin.read() == ""  # all input consumed

    wf_spec = get_serializable(OrderedDict(), serialization_settings, wf)
    assert len(wf_spec.template.nodes) == 5
    # The first t1 call
    assert wf_spec.template.nodes[0].task_node is not None

    # The first signal s1, dependent on the first t1 call
    assert wf_spec.template.nodes[1].upstream_node_ids == ["n0"]
    assert wf_spec.template.nodes[1].gate_node is not None
    assert wf_spec.template.nodes[1].gate_node.signal.signal_id == "my-signal-name"
    assert wf_spec.template.nodes[1].gate_node.signal.type.simple == 4
    assert wf_spec.template.nodes[1].gate_node.signal.output_variable_name == "o0"

    # The second signal
    assert wf_spec.template.nodes[2].upstream_node_ids == []
    assert wf_spec.template.nodes[2].gate_node is not None
    assert wf_spec.template.nodes[2].gate_node.signal.signal_id == "my-signal-name-2"
    assert wf_spec.template.nodes[2].gate_node.signal.type.simple == 1
    assert wf_spec.template.nodes[2].gate_node.signal.output_variable_name == "o0"

    # The second call to t1, dependent on the first signal
    assert wf_spec.template.nodes[3].upstream_node_ids == ["n1"]
    assert wf_spec.template.nodes[3].task_node is not None

    # The call to t2, dependent on the second signal
    assert wf_spec.template.nodes[4].upstream_node_ids == ["n2"]
    assert wf_spec.template.nodes[4].task_node is not None

    assert wf_spec.template.outputs[0].binding.promise.node_id == "n4"
    assert wf_spec.template.outputs[1].binding.promise.node_id == "n3"

    #     c = conditional("use_gate").if_(x is True). \
    #             then(t1(y)). \
    #             else_(). \
    #             fail("failure message") \
    #
    # (
    #     conditional("fractions")
    #         .if_((my_input > 0.1) & (my_input < 1.0))
    #         .then(double(n=my_input))
    #         .elif_((my_input > 1.0) & (my_input < 10.0))
    #         .then(square(n=my_input))
    #         .else_()
    #         .fail("The input must be between 0 and 10")
    # )

    # @workflow
    # def wf_sleep():
    #     x = flyte.sleep("10s")
    #     b = t1(a=a)
    #
    #     x >> b
