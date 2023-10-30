.. _artifacts:

###########################
Flyte Artifacts and Data
###########################


************
Background
************
Flyte produces data as part of its normal operation, as outputs of tasks and workflows. As the project and community and usage grew, we realized that this data is not well tracked after it’s produced. Previously, there were no easy ways to share, access, or track this data. If a task creates a file or a folder, this would get uploaded to the configured blob store, but unfortunately that's where Flyte’s association with that data stopped. Users that wish to share or just record this data either would have to keep a record of the S3 path, or keep a link to the original execution to find it again. Flyte artifacts addresses this shortcoming, and in doing so, enables a broad set of different use-cases.

todo:
(information flow diagram copy from)

Concepts
========
What is an Artifact?
^^^^^^^^^^^^^^^^^^^^

An Artifact is just metadata that is attached to precisely one Flyte Literal value. A literal value is just the output of a task, like the number 42, or the spark dataframe residing at some ``s3://bucket/location``. Typically these literals are outputs of Flyte tasks and workflows, but user uploads used as inputs to workflows are also tracked.

Partitions
^^^^^^^^^^
Artifacts can be partitioned. A partition is just a key value pair of strings that has some semantic meaning to the user.

ID and Versioning
^^^^^^^^^^^^^^^^^
An Artifact must have a name, and like all other Flyte entities, have a project and domain and a version. The version is something.


******************
Basic Usage
******************

Declaration
===============
Three options are available to declare artifacts in your flytekit code that range in degrees of flexibility and the changes that would be required to existing code.

Using Annotated
^^^^^^^^^^^^^^^
This is the simplest option in that it requires the least amount of changes to existing code. All you need to do is declare the name and its partitions, if any. To attach it to the output of a task, just add it to ``Annotated``.

.. code-block:: python

    from typing_extensions import Annotated

    from flytekit import task, Artifact, Inputs
    my_artifact = Artifact(name="my_data", partition_keys=["a", "b"], time_partitioned=True)

    @task
    def my_task(b_value: str, run_time: datetime) -> Annotated[pd.DataFrame, my_artifact(b=Inputs.b_value).bind_time_partition(Inputs.run_time)]:
        df = ...
        return df

When this task runs, the dataframe output will be registered as an artifact with the name ``my_data`` and a partition key ``b`` with the value of whatever the input for ``b_value`` was. There is also a time partition that is set to the value of the ``run_time`` input.

With this approach, the value of the partitions must either be static, or bound to an input. They cannot be generated dynamically at run time.


Specifying Partitions at Execution Time
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    my_artifact = Artifact[pd.DataFrame](name="flyteorg.test.yt.test", partition_keys=["a", "b"])

    @task
    def base_t2_orig(b_value: str) -> Annotated[pd.DataFrame, my_artifact]:
        df = ...
        return my_artifact(df, a="a_value", b=b_value+"2")

This way requires a bit more code change to existing code but is more flexible because it allows users to specify partition values at run time and this form factor can also be used to add generic metadata to the artifact. It is important that the first argument to the call must be the original value your task returned.

<Add model/dataset card usage here when ready>


Artifact as a Type
^^^^^^^^^^^^^^^^^^
This option may make the most sense when writing new code. You can declare your own type, and pass that around as inputs. The compiled form of the tasks and workflows that reference the artifact will still have the same Flyte type had there been no Artifact of course. The example here shows a workflow that returns two outputs.

.. code-block:: python

    class MyData(Artifact[pd.DataFrame]):
        partitions = ["a", "b"]

    @workflow
    def upstream_wf(input_a: datetime) -> typing.Tuple[int, MyData]:
        int_result = some_task()
        df_result = some_task_2()
        str_result = some_task_3()
        return int_result, MyData(df_result, a="constant str okay", b=Inputs.input_a)

Note here that as this is a workflow, and workflows are only compiled by Python and not run by Python (except in local execution), you can't do have dynamic partition values like the ``+2`` example above. Downstream tasks and workflows should take the ``MyData`` type as the type.

Interactive Usage
==================
One of the benefits of using artifacts is that they make usage in interactive environments easier, like when trying to download outputs of an existing task.

Downloading
^^^^^^^^^^^

Launching New Executions
^^^^^^^^^^^^^^^^^^^^^^^^


Parameters to Launch Plans
==========================
Having data as first-class citizens in Flyte in the form of Artifacts means that you can now use that data as a means for more flexible coupling between disparate workflows. For example, assume you had an artifact produced by one team.

.. code-block:: python

    RideCountData = Artifact(
        name="ride_count_data",
        time_partitioned=True,
        partition_keys=["region"],
    )

And another team would like to use it. Rather than writing an explicit parent workflow that links the two together, you can use an artifact query as an input to the second workflow.

.. code-block:: python

    data_query = RideCountData.query(region=Inputs.region, time_partition=Inputs.kickoff_time)

    @workflow
    def run_train_model(region: str, kickoff_time: datetime, data: pd.DataFrame = data_query):
        train_model(region=region, data=data)

This reads, at execution time, look for the most up to date artifact with the name ``ride_count_data`` with the partition values inferred by the other inputs. If there is no such artifact, the workflow will fail (unless the input is optional, in which case ``None`` will be used).

******************
Trigger Usage
******************

