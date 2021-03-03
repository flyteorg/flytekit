from tests.flytekit.unit.core.tracker.b import b_local_a
from tests.flytekit.unit.core.tracker.c import c_local_a


def test_tracking():
    # Test that instantiated in returns the module (.py file) where the instance is instantiated, not where the class
    # is defined.
    assert b_local_a.instantiated_in == "tests.flytekit.unit.core.tracker.b"
    assert b_local_a.lhs == "b_local_a"

    # Test that even if the actual declaration that constructs the object is in a different file, instantiated_in
    # still shows the module where the Python file where the instance is assigned to a variable
    assert c_local_a.instantiated_in == "tests.flytekit.unit.core.tracker.c"
    assert c_local_a.lhs == "c_local_a"
