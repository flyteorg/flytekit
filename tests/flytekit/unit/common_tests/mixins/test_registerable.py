from __future__ import absolute_import

from tests.flytekit.unit.common_tests.mixins import sample_registerable as _sample_registerable


def test_instance_tracker():
    assert _sample_registerable.example.instantiated_in == "tests.flytekit.unit.common_tests.mixins.sample_registerable"


def test_auto_name_assignment():
    _sample_registerable.example.auto_assign_name()
    assert _sample_registerable.example.platform_valid_name == \
           "tests.flytekit.unit.common_tests.mixins.sample_registerable.example"
