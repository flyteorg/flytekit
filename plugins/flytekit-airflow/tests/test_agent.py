from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor
from airflow.sensors.time_sensor import TimeSensor
from pytz import UTC

from flytekit import workflow


def py_func():
    print("airflow python sensor")
    return True


@workflow
def wf():
    sensor = TimeSensor(task_id="fire_immediately", target_time=(datetime.now(tz=UTC) + timedelta(seconds=1)).time())
    t3 = BashSensor(task_id="Sensor_succeeds", bash_command="exit 0")
    foo = PythonOperator(task_id="foo", python_callable=py_func)
    sensor >> t3 >> foo


def test_airflow_agent():
    wf()
