import os as _os

from pyspark import SparkConf, SparkContext


# TODO: Support Client Mode
def get_spark_context(spark_conf):
    """
       outputs: SparkContext
       Returns appropriate SparkContext based on whether invoked via a Notebook or a Flyte workflow.
    """
    # We run in cluster-mode in Flyte.
    # Ref https://github.com/lyft/flyteplugins/blob/master/go/tasks/v1/flytek8s/k8s_resource_adds.go#L46
    if "FLYTE_INTERNAL_EXECUTION_ID" in _os.environ:
        return SparkContext()

    # Add system spark-conf for local/notebook based execution.
    spark_conf.add(("spark.master", "local"))
    conf = SparkConf().setAll(spark_conf)
    return SparkContext(conf=conf)
