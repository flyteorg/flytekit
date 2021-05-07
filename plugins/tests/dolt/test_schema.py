import pytest
from flytekitplugins.dolt.schema import DoltTable, DoltTableNameTransformer
from google.protobuf.struct_pb2 import Struct

from flytekit.models.literals import Literal, Scalar


def test_dolt_table_to_python_value():
    with pytest.raises(AssertionError):
        DoltTableNameTransformer.to_literal(
            self=None,
            ctx=None,
            python_val=None,
            python_type=DoltTable,
            expected=None,
        )


def test_dolt_table_to_literal():
    s = Struct()
    s.update({"dummy": "data"})
    lv = Literal(Scalar(generic=s))

    res = DoltTableNameTransformer.to_python_value(
        self=None,
        ctx=None,
        lv=lv,
        expected_python_type=DoltTable,
    )
    assert res == lv
