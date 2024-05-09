from enum import Enum
from io import StringIO

import pandas as pd

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Aggregation(Enum):
    """Aggregation types."""

    FIRST = "first"
    LAST = "last"
    MEAN = "mean"


def influx_json_to_df(json_data: str) -> pd.DataFrame:
    """Converts JSON data output of InfluxDBAgent to a Pandas DataFrame.

    Args:
        json_data (str): JSON data output of InfluxDBAgent.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the queried data.
    """
    df = pd.read_json(StringIO(json_data))
    return df.drop(columns=["table", "result", "_start", "_stop", "_measurement"])
