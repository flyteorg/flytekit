from flytekit.contrib.base_sensor import Sensor as _Sensor
from flytekit.contrib.task import sensor_task


class MyMockSensor(_Sensor):

    def __init__(self, **kwargs):
        super(MyMockSensor, self).__init__(**kwargs)

    def _do_poll(self):
        """
        :rtype: (bool, Optional[datetime.timedelta])
        """
        return True, None


def test_sensor_works():
    @sensor_task
    def my_test_task(wf_params):
        return MyMockSensor()

    out = my_test_task.unit_test()
    assert len(out) == 0
