from flytekit.annotated.sample import x
from flytekit.configuration.common import CONFIGURATION_SINGLETON


CONFIGURATION_SINGLETON.x = 0


def test_fds():
    r = x(s=33)
    print(r)

    # print(y(5))
    # print(z(6))


from tests.flytekit.common.workflows.simple import add_one


def test_mcds():
    c = add_one(a=1)
    x = 5

