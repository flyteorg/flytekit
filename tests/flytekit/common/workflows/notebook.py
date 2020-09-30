#
# interactive_python = python_notebook(
#     notebook_path="../../../../notebook-task-examples/python-notebook.ipynb",
#     inputs=inputs(pi=Types.Float),
#     outputs=outputs(out=Types.Float),
#     cpu_request="1",
#     memory_request="1G",
# )
#
# interactive_spark = spark_notebook(
#     notebook_path="../../../../notebook-task-examples/spark-notebook-pi.ipynb",
#     inputs=inputs(partitions=Types.Integer),
#     outputs=outputs(pi=Types.Float),
# )
#
#
# @workflow_class
# class FlyteNotebookSparkWorkflow(object):
#     partitions = Input(Types.Integer, default=10)
#     out1 = interactive_spark(partitions=partitions)
#     out2 = interactive_python(pi=out1.outputs.pi)
