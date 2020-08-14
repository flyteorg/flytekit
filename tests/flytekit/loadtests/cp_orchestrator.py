from __future__ import absolute_import, division, print_function

from six.moves import range

from flytekit.sdk.workflow import workflow_class
from tests.flytekit.loadtests.cp_python import FlyteCPPythonLoadTestWorkflow
from tests.flytekit.loadtests.cp_spark import FlyteCPSparkLoadTestWorkflow

# launch plans for individual load tests.
python_loadtest_lp = FlyteCPPythonLoadTestWorkflow.create_launch_plan()
spark_loadtest_lp = FlyteCPSparkLoadTestWorkflow.create_launch_plan()


# Orchestrator workflow invokes the individual load test workflows (hive, python, spark). Its static for now but we
# will make it dynamic in future
@workflow_class
class CPLoadTestOrchestrationWorkflow(object):

    # python load tests. 5 tasks each. Total: 1 cpu 5gb memory per python workflow.
    python_task_count = 50
    p = [None] * python_task_count

    for i in range(0, python_task_count):
        p[i] = python_loadtest_lp()

    # spark load tests.
    spark_task_count = 30
    s = [None] * spark_task_count

    for i in range(0, spark_task_count):
        s[i] = spark_loadtest_lp()
