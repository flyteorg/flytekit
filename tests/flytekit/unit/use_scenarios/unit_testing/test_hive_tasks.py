from __future__ import absolute_import

import pytest

from flytekit.sdk.tasks import hive_task


def test_no_queries():
    @hive_task
    def test_hive_task(wf_params):
        pass

    assert test_hive_task.unit_test() == []


def test_empty_list_queries():
    @hive_task
    def test_hive_task(wf_params):
        return []

    assert test_hive_task.unit_test() == []


def test_one_query():
    @hive_task
    def test_hive_task(wf_params):
        return "abc"

    assert test_hive_task.unit_test() == ["abc"]


def test_multiple_queries():
    @hive_task
    def test_hive_task(wf_params):
        return ["abc", "cde"]

    assert test_hive_task.unit_test() == ["abc", "cde"]


def test_raise_exception():
    @hive_task
    def test_hive_task(wf_params):
        raise FloatingPointError("Floating point error for some reason.")

    with pytest.raises(FloatingPointError):
        test_hive_task.unit_test()
