from collections import OrderedDict
from datetime import timedelta

import flytekit.configuration
from flytekit.configuration import Image, ImageConfig
from flytekit.core.gate import signal
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



def test_asd():
    @task
    def t1(a: int):
        print(a)

    @workflow
    def wf(a: int) -> bool:
        t1(a=a)
        x = signal("my-signal-name", timeout=timedelta(hours=1), expected_type=bool)
        y = signal("my-signal-name-2", timeout=timedelta(hours=2), expected_type=int)
        t1(a=y)
        return x

    res = wf(a=5)
    print(res)

    wf_spec = get_serializable(OrderedDict(), serialization_settings, wf)
    print(wf_spec)

    #     c = conditional("use_gate").if_(x is True). \
    #             then(t1(y)). \
    #             else_(). \
    #             fail("failure message") \
    #
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

