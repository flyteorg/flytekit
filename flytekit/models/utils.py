import datetime
from datetime import timedelta
from datetime import timezone as _timezone

import flyteidl_rust as flyteidl

## TODO: refator for interface consistency


# A helping function for `from_flyte_idl()` to convert to Python `datetime`.
def convert_to_datetime(seconds: int, nanos: int) -> datetime.datetime:
    total_microseconds = (seconds * 1_000_000) + (nanos // 1_000)
    dt = (datetime.datetime(1970, 1, 1) + timedelta(microseconds=total_microseconds)).replace(tzinfo=_timezone.utc)
    return dt


# A function to convert Python `timedelta` `to flyteidl.protobuf.Duration(seconds, nanoseconds)`.
def convert_from_timedelta_to_duration(delta: datetime.timedelta) -> flyteidl.protobuf.Duration:
    total_seconds = delta.total_seconds()
    seconds = int(total_seconds)
    nanos = int((total_seconds - seconds) * 1e9)  # Convert the fractional part to nanoseconds
    return flyteidl.protobuf.Duration(seconds=seconds, nanos=nanos)


# A function to convert Python `datetime` `to flyteidl.protobuf.Timestamp(seconds, nanoseconds)`.
def convert_from_datetime_to_timestamp(dt: datetime.datetime) -> flyteidl.protobuf.Timestamp:
    # Ensure the datetime is in UTC and is timezone-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)
    # Convert to seconds since the epoch
    total_seconds = dt.timestamp()
    seconds = int(total_seconds)
    nanos = int(
        (total_seconds - seconds) * 1e9
    )  # Fractional part to nanoseconds  # Convert the fractional part to nanoseconds

    return flyteidl.protobuf.Timestamp(seconds=seconds, nanos=nanos)
