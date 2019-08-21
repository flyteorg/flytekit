from __future__ import absolute_import, division, print_function

from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input
from tests.flytekit.loadtests.python import FlytePythonLoadTestWorkflow
from tests.flytekit.loadtests.hive import FlyteHiveLoadTestWorkflow
from tests.flytekit.loadtests.spark import FlyteSparkLoadTestWorkflow
from tests.flytekit.loadtests.dynamic_job import FlyteDJOLoadTestWorkflow

from six.moves import range

# launch plans for individual load tests.
python_loadtest_lp = FlytePythonLoadTestWorkflow.create_launch_plan()
hive_loadtest_lp = FlyteHiveLoadTestWorkflow.create_launch_plan()
spark_loadtest_lp = FlyteSparkLoadTestWorkflow.create_launch_plan()
dynamic_job_loadtest_lp = FlyteDJOLoadTestWorkflow.create_launch_plan()


# Orchestrator workflow invokes the individual load test workflows (hive, python, spark). Its static for now but we
# will make it dynamic in future,.
@workflow_class
class LoadTestOrchestrationWorkflow(object):

    # 30 python tasks ~=  75 i3.16x nodes on AWS Batch
    python_task_count = 30
    # 30 spark tasks ~=  60 i3.16x nodes on AWS Batch
    spark_task_count = 30
    # 3 dynamic-jobs each of 1000 tasks ~=  3*20 i3.16x nodes on AWS Batch
    djo_task_count = 1000
    dj_count = 3

    p = [None] * python_task_count
    s = [None] * spark_task_count
    d = [None] * dj_count

    # python tasks
    for i in range(0, python_task_count):
        p[i] = python_loadtest_lp()

    # dynamic-job tasks
    for i in range(0, dj_count):
        d[i] = dynamic_job_loadtest_lp(tasks_count=djo_task_count)

    # hive load tests.
    # h1 = hive_loadtest_lp()

    # spark load tests
    trigger_time = Input(Types.Datetime)
    for i in range(0, spark_task_count):
        s[i] = spark_loadtest_lp(triggered_date=trigger_time, offset=i)
