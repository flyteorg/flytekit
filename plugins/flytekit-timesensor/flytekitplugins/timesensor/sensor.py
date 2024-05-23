from flytekit.sensor.base_sensor import BaseSensor
import time


class TimeDeltaSensor(BaseSensor):
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, **kwargs)

    async def poke(self, timeDelta: int) -> bool:
        print("Before the sleep sensor")
        time.sleep(timeDelta)
        print("After the time sleep sensor")
        return True
