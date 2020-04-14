from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from flytekit.sdk.tasks import generic_spark_task, inputs, python_task
from flytekit.sdk.types import Types
from flytekit.sdk.spark_types import SparkType
from flytekit.sdk.workflow import workflow_class, Input


scala_spark = generic_spark_task(
    spark_type=SparkType.SCALA,
    inputs=inputs(partitions=Types.Integer),
    main_class="org.apache.spark.examples.SparkPi",
    main_application_file="local:///opt/spark/examples/jars/spark-examples.jar",
    spark_conf={
                'spark.driver.memory': "1000M",
                'spark.executor.memory': "1000M",
                'spark.executor.cores': '1',
                'spark.executor.instances': '2',
            },
    cache_version='1'
)


@inputs(date_triggered=Types.Datetime)
@python_task(cache_version='1')
def print_every_time(workflow_parameters, date_triggered):
    print("My input : {}".format(date_triggered))


@workflow_class
class SparkTasksWorkflow(object):
    triggered_date = Input(Types.Datetime)
    partitions = Input(Types.Integer)
    sparkTask = scala_spark(partitions=partitions)
    print_always = print_every_time(
        date_triggered=triggered_date)
