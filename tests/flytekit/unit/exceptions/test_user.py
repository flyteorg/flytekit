from flytekit.exceptions import base, user


def test_flyte_user_exception():
    try:
        base_exn = Exception("everywhere is bad")
        raise user.FlyteUserException("bad") from base_exn
    except Exception as e:
        assert str(e) == "USER:Unknown: error=bad, cause=everywhere is bad"
        assert isinstance(type(e), base._FlyteCodedExceptionMetaclass)
        assert type(e).error_code == "USER:Unknown"
        assert isinstance(e, base.FlyteException)


def test_flyte_type_exception():
    try:
        base_exn = Exception("Types need to be right!!!")
        raise user.FlyteTypeException(
            "int", "float", received_value=1, additional_msg="That was a bad idea!"
        ) from base_exn
    except Exception as e:
        assert (
            str(e) == "USER:TypeError: error=Type error!  Received: int with value: 1, Expected: float. "
            "That was a bad idea!, cause=Types need to be right!!!"
        )
        assert isinstance(e, TypeError)
        assert type(e).error_code == "USER:TypeError"
        assert isinstance(e, user.FlyteUserException)

    try:
        base_exn = Exception("everyone is upset with you")
        raise user.FlyteTypeException(
            "int",
            ("list", "set"),
            received_value=1,
            additional_msg="That was a bad idea!",
        ) from base_exn
    except Exception as e:
        assert (
            str(e)
            == "USER:TypeError: error=Type error!  Received: int with value: 1, Expected one of: ('list', 'set'). That was a "
            "bad idea!, cause=everyone is upset with you"
        )
        assert isinstance(e, TypeError)
        assert type(e).error_code == "USER:TypeError"
        assert isinstance(e, user.FlyteUserException)

    try:
        base_exn = Exception("int != float")
        raise user.FlyteTypeException("int", "float", additional_msg="That was a bad idea!") from base_exn
    except Exception as e:
        assert (
            str(e)
            == "USER:TypeError: error=Type error!  Received: int, Expected: float. That was a bad idea!, cause=int != float"
        )
        assert isinstance(e, TypeError)
        assert type(e).error_code == "USER:TypeError"
        assert isinstance(e, user.FlyteUserException)

    try:
        raise user.FlyteTypeException("int", ("list", "set"), additional_msg="That was a bad idea!")
    except Exception as e:
        assert (
            str(e) == "USER:TypeError: error=Type error!  Received: int, Expected one of: ('list', 'set'). That was a "
            "bad idea!"
        )
        assert isinstance(e, TypeError)
        assert type(e).error_code == "USER:TypeError"
        assert isinstance(e, user.FlyteUserException)


def test_flyte_value_exception():
    try:
        base_exn = Exception("live up to expectations")
        raise user.FlyteValueException(-1, "Expected a value > 0") from base_exn
    except user.FlyteValueException as e:
        assert (
            str(e)
            == "USER:ValueError: error=Value error!  Received: -1. Expected a value > 0, cause=live up to expectations"
        )
        assert isinstance(e, ValueError)
        assert type(e).error_code == "USER:ValueError"
        assert isinstance(e, user.FlyteUserException)


def test_flyte_assert():
    try:
        base_exn = Exception("!!!!!")
        raise user.FlyteAssertion("I ASSERT THAT THIS IS WRONG!") from base_exn
    except user.FlyteAssertion as e:
        assert str(e) == "USER:AssertionError: error=I ASSERT THAT THIS IS WRONG!, cause=!!!!!"
        assert isinstance(e, AssertionError)
        assert type(e).error_code == "USER:AssertionError"
        assert isinstance(e, user.FlyteUserException)


def test_flyte_validation_error():
    try:
        base_exn = Exception("somewhere else said this isn't valid")
        raise user.FlyteValidationException("I validated that your stuff was wrong.") from base_exn
    except user.FlyteValidationException as e:
        assert (
            str(e)
            == "USER:ValidationError: error=I validated that your stuff was wrong., cause=somewhere else said this isn't valid"
        )
        assert isinstance(e, AssertionError)
        assert type(e).error_code == "USER:ValidationError"
        assert isinstance(e, user.FlyteUserException)
