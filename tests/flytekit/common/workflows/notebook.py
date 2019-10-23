from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input
from flytekit.common.tasks.notebook import python_notebook, spark_notebook

interactive_python = python_notebook(notebook_path="../../../../notebooks/python-notebook.ipynb",
                                          inputs={"pi": Types.Float},
                                          outputs={"out": Types.Float},
                                          cpu_request="1",
                                          memory_request="1G"
                                        )

interactive_spark = spark_notebook(notebook_path="../../../../notebooks/spark-notebook-pi.ipynb",
                                          inputs={"partitions": Types.Integer},
                                          outputs={"pi": Types.Float}
                                        )

@workflow_class
class FlyteNotebookSparkWorkflow(object):
    partitions = Input(Types.Integer, default=10)
    out1 = interactive_spark(partitions=partitions)
    out2 = interactive_python(pi=out1.outputs.pi)
    