from __future__ import absolute_import, division, print_function

import random
from operator import add

from six.moves import range

from flytekit.sdk.tasks import inputs, outputs, spark_task
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class


@inputs(partitions=Types.Integer)
@outputs(out=Types.Float)
@spark_task(
    spark_conf={
        "spark.driver.memory": "600M",
        "spark.executor.memory": "600M",
        "spark.executor.cores": "1",
        "spark.executor.instances": "1",
        "spark.hadoop.mapred.output.committer.class": "org.apache.hadoop.mapred.DirectFileOutputCommitter",
        "spark.hadoop.mapreduce.use.directfileoutputcommitter": "true",
    },
    cache_version="1",
)
def hello_spark(workflow_parameters, spark_context, partitions, out):
    print("Starting Spark with Partitions: {}".format(partitions))

    n = 30000 * partitions
    count = spark_context.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    pi_val = 4.0 * count / n
    print("Pi val is :{}".format(pi_val))
    out.set(pi_val)


def f(_):
    x = random.random() * 2 - 1
    y = random.random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0


@workflow_class
class FlyteCPSparkLoadTestWorkflow(object):
    sparkTask = hello_spark(partitions=50)
