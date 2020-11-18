import re
from typing import Any, Dict, Type

from flytekit.annotated.base_task import kwtypes, PythonTask
from flytekit.annotated.interface import Interface
from flytekit.models import task as _task_model


class SQLTask(PythonTask):
    """
    Base task types for all SQL tasks
    """

    # TODO this should be replaced with Schema Type
    _OUTPUTS = kwtypes(results=str)
    _INPUT_REGEX = re.compile(r"({{\s*.inputs.(\w+)\s*}})", re.IGNORECASE)

    def __init__(
        self,
        name: str,
        query_template: str,
        inputs: Dict[str, Type],
        metadata: _task_model.TaskMetadata,
        task_type="sql_task",
        *args,
        **kwargs,
    ):
        super().__init__(
            task_type=task_type,
            name=name,
            interface=Interface(inputs=inputs, outputs=self._OUTPUTS),
            metadata=metadata,
            *args,
            **kwargs,
        )
        self._query_template = query_template

    @property
    def query_template(self) -> str:
        return self._query_template

    def execute(self, **kwargs) -> Any:
        modified_query = self._query_template
        matched = set()
        for match in self._INPUT_REGEX.finditer(self._query_template):
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
            raise ValueError(f"Extra Inputs have not matches in query template - missing {diff}")
        return None
