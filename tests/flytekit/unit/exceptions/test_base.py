from flytekit.exceptions import base


def test_flyte_exception():
    try:
        raise base.FlyteException("bad")
    except Exception as e:
        assert str(e) == "UnknownFlyteException: error=bad"
        assert isinstance(type(e), base._FlyteCodedExceptionMetaclass)
        assert type(e).error_code == "UnknownFlyteException"


def test_flyte_exception_cause():
    try:
        base_exn = Exception("exception from somewhere else")
        raise base.FlyteException("bad") from base_exn
    except Exception as e:
        assert str(e) == "UnknownFlyteException: error=bad, cause=exception from somewhere else"
        assert isinstance(type(e), base._FlyteCodedExceptionMetaclass)
        assert type(e).error_code == "UnknownFlyteException"
