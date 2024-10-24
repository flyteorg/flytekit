from typing import Optional


class _FlyteCodedExceptionMetaclass(type):
    @property
    def error_code(cls):
        return cls._ERROR_CODE


class FlyteException(Exception, metaclass=_FlyteCodedExceptionMetaclass):
    _ERROR_CODE = "UnknownFlyteException"

    def __init__(self, *args, timestamp: Optional[float] = None) -> None:
        super().__init__(*args)
        self._timestamp = timestamp

    @property
    def timestamp(self) -> Optional[float]:
        """
        The timestamp as fractional seconds since epoch
        """
        return self._timestamp

    def __str__(self):
        error_message = f"error={','.join(self.args) if self.args else 'None'}"
        if self.__cause__:
            error_message += f", cause={self.__cause__}"

        return f"{self._ERROR_CODE}: {error_message}"


class FlyteRecoverableException(FlyteException):
    _ERROR_CODE = "RecoverableFlyteException"
