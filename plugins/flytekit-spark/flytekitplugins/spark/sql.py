from flytekit.types.structured import StructuredDataset

from flytekit.core.base_sql_task import SQLTask
from flytekit.extras.tasks.shell import _PythonFStringInterpolizer
from .task import PysparkFunctionTask, Spark

class SparkSQLTask(PysparkFunctionTask, SQLTask[Spark]):
    """
    A wrapper task to easily run SparkSQL commands that are automatically interpolated by Flyte
    """

    def __init__(
        self,
        name: str,
        query_template: str,
        task_config: Spark,
        inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        output_schema_type: typing.Optional[typing.Type[StructuredDataset]] = None,
        container_image: str = None,
        use_flyte_native_interpolizer: bool = False, # Defaults to using python interpolizer
        **kwargs,
    ):
        output_schema = output_schema_type if output_schema_type else StructuredDataset
        outputs = kwtypes(results=output_schema)
        self._use_flyte_native_interpolizer = use_flyte_native_interpolizer

        super().__init__(
            name=name,
            task_config=task_config,
            task_type=self._SPARK_TASK_TYPE,
            query_template=query_template,
            container_image=container_image,
            inputs=inputs,
            outputs=outputs,
            **kwargs,
        )

    def execute(self, **kwargs) -> Any:
        sess = flytekit.current_context().spark_session
        if self._use_flyte_native_interpolizer:
            final_query = self.interpolate_query(query_template=self.query_template, **kwargs)
        else:
            final_query = _PythonFStringInterpolizer().interpolate(self.query_template, inputs=kwargs)
        df = sess.sql(final_query)
        return df





if __name__ == "__main__":
    s = SparkSQLTask(
        query_template=f'''
        Select * from {table}
        ''',
        inputs=kwtypes(table=str),
    )
    s()