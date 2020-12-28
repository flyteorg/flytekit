import re
from typing import Any, Dict, Type

from flytekit.annotated.base_task import PythonTask
from flytekit.annotated.interface import Interface
from flytekit.models import task as _task_model


class SQLTask(PythonTask):
    """
    Base task types for all SQL tasks
    """

    _INPUT_REGEX = re.compile(r"({{\s*.inputs.(\w+)\s*}})", re.IGNORECASE)

    def __init__(
        self,
        name: str,
        metadata: _task_model.TaskMetadata,
        query_template: str,
        inputs: Dict[str, Type],
        outputs: Dict[str, Type] = None,
        task_type="sql_task",
        *args,
        **kwargs,
    ):
        super().__init__(
            task_type=task_type,
            name=name,
            interface=Interface(inputs=inputs, outputs=outputs or {}),
            metadata=metadata,
            *args,
            **kwargs,
        )
        self._query_template = query_template

    @property
    def query_template(self) -> str:
        return self._query_template

    def execute(self, **kwargs) -> Any:
        raise Exception("Cannot run a SQL Task natively, please mock.")

    def get_query(self, **kwargs) -> str:
        return self.interpolate_query(self.query_template, **kwargs)

    @classmethod
    def interpolate_query(cls, query_template, **kwargs) -> Any:
        """
        This function will fill in the query template with the provided kwargs and return the interpolated query
        Please note that when SQL tasks run in Flyte, this step is done by the
        """
        modified_query = query_template
        matched = set()
        for match in cls._INPUT_REGEX.finditer(query_template):
            expr = match.groups()[0]
            var = match.groups()[1]
            if var not in kwargs:
                raise ValueError(f"Variable {var} in Query (part of {expr}) not found in inputs {kwargs.keys()}")
            matched.add(var)
            val = kwargs[var]
            # str conversion should be deliberate, with right conversion for each type
            modified_query = modified_query.replace(expr, str(val))

        if len(matched) < len(kwargs.keys()):
            diff = set(kwargs.keys()).difference(matched)
            raise ValueError(f"Extra Inputs have no matches in query template - missing {diff}")
        return modified_query
