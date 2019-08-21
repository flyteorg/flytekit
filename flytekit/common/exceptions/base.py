from __future__ import absolute_import
import six as _six


class _FlyteCodedExceptionMetaclass(type):
    @property
    def error_code(cls):
        return cls._ERROR_CODE


class FlyteException(_six.with_metaclass(_FlyteCodedExceptionMetaclass, Exception)):
    _ERROR_CODE = 'UnknownFlyteException'


class FlyteRecoverableException(FlyteException):
    _ERROR_CODE = "RecoverableFlyteException"
