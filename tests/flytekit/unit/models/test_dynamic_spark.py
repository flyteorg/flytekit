from configuration import TemporaryConfiguration
from flytekit.common import constants as _sdk_constants
from flytekit.sdk import tasks as _tasks
from flytekit.sdk.types import Types as _Types


@_tasks.outputs(o=_Types.Integer)
@_tasks.inputs(i=_Types.Integer)
@_tasks.spark_task(spark_conf={"x": "y"})
def my_spark_task(ctx, sc, i, o):
    pass


@_tasks.inputs(num=_Types.Integer)
@_tasks.outputs(out=_Types.Integer)
@_tasks.dynamic_task
def spark_yield_task(wf_params, num, out):
    wf_params.logging.info("Running inner task... yielding a launchplan")
    t = my_spark_task.with_overrides(new_spark_conf={"a": "b"})
    o = t(i=num)
    yield o
    out.set(o.outputs.o)


def test_spark_yield():
    with TemporaryConfiguration(None, internal_overrides={"image": "fakeimage"}):
        outputs = spark_yield_task.unit_test(num=1)
        dj_spec = outputs[_sdk_constants.FUTURES_FILE_NAME]
        print(dj_spec)
