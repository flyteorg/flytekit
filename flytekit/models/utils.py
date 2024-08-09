import datetime
from datetime import timedelta
from datetime import timezone as _timezone


# A helping function for `from_flyte_idl()` to convert to Python `datetime`.
def convert_to_datetime(seconds: int, nanos: int) -> datetime.datetime:
    total_microseconds = (seconds * 1_000_000) + (nanos // 1_000)
    dt = (datetime.datetime(1970, 1, 1) + timedelta(microseconds=total_microseconds)).replace(tzinfo=_timezone.utc)
    return dt
