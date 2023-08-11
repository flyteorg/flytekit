import tempfile

from flytekit import task, workflow
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.sensor.file_sensor import FileSensor, FileSensorConfig
from tests.flytekit.unit.test_translator import default_img


def test_sensor_task():
    path = "/tmp/12345"
    sensor = FileSensor(path=path)
    assert sensor.task_type == "file_sensor"
    assert sensor.task_config == FileSensorConfig(path)
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    assert sensor.get_custom(settings) == {"path": path}

    tmp_file = tempfile.NamedTemporaryFile()
    sensor = FileSensor(path=tmp_file.name)

    @task()
    def t1():
        print("flyte")

    @workflow
    def wf():
        sensor() >> t1()

    if __name__ == "__main__":
        wf()
