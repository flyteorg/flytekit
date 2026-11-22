# Flytekit InfluxDB Plugin

InfluxDB, coupled with the power of pandas, facilitates streamlined management and analysis of time-series data.
By integrating the InfluxDB plugin with Flyte, you gain access to time-series data manipulation capabilities directly within your workflows.
This plugin is tailored to interact with pandas DataFrames, allowing for creation, retrieval, and deletion of time-series data stored in InfluxDB.

## Example
```python
from datetime import datetime, timedelta
from flytekit import task, workflow
from flytekitplugins.influxdb import InfluxDBTask, Aggregation, influx_json_to_df

influx_query_task = InfluxDBTask(
    name="influx_task",
    url="http://my-influxdb.com",
    org="my_org")

@workflow
def wf(bucket: str, measurement: str) -> str:
    start_time = datetime(2024, 4, 25, 0, 0, 0)
    data = influx_query_task(
        bucket=bucket,
        measurement=measurement,
        start_time=start_time,
        end_time=start_time + timedelta(days=2),
        tag_dict={"my_tags": ["tag_1", "tag_2"]},
        fields=["fields_1", "fields_2"],
        aggregation=Aggregation.LAST,
        period_min=30
    )
    return influx_json_to_df(data)


if __name__ == "__main__":
    print(wf(bucket="my_bucket", measurement="my_measurement"))
```

To install the plugin, run the following command:

```bash
pip install flytekitplugins-influxdb
```

To configure InfluxDB in the Flyte deployment's backend, follow the [configuration guide](none).
