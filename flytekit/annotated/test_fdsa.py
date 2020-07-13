from flytekit.annotated.sample import x, my_workflow
from flytekit.configuration.common import CONFIGURATION_SINGLETON
from tests.flytekit.common.workflows.simple import add_one


CONFIGURATION_SINGLETON.x = 0


def test_fds():
    r = x(s=33)
    print(r)


def test_mcds():
    c = add_one(a=1)
    x = 5


def test_www():
    x = my_workflow()
