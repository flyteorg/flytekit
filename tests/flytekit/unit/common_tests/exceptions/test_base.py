from __future__ import absolute_import

from flytekit.common.exceptions import base


def test_flyte_exception():
    try:
        raise base.FlyteException("bad")
    except Exception as e:
        assert str(e) == "bad"
        assert isinstance(type(e), base._FlyteCodedExceptionMetaclass)
        assert type(e).error_code == "UnknownFlyteException"
