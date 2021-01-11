import json
import logging
import os
import typing
from typing import Any

from flytekit import FlyteContext
from flytekit.annotated.interface import Interface
from flytekit.annotated.python_function_task import PythonInstanceTask
from flytekit.annotated.task import TaskPlugins
from flytekit.annotated.type_engine import TypeEngine
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
import papermill as pm
from nbconvert import HTMLExporter

from flytekit.models.literals import LiteralMap
from flyteidl.core.literals_pb2 import LiteralMap as _pb2_lm

T = typing.TypeVar("T")


def _dummy_task_func():
    return None


class NotebookTask(PythonInstanceTask[T]):

    _IMPLICIT_OP_NOTEBOOK = "out_notebook"
    _IMPLICIT_RENDERED_NOTEBOOK = "out_rendered_nb"
    _IMPLICIT_RENDERED_NOTEBOOK_TYPE = 

    def __init__(self,
                 name: str,
                 task_config: T,
                 notebook_path: str,
                 inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
                 outputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
                 **kwargs):
        plugin_class = TaskPlugins.find_pythontask_plugin(type(task_config))
        self._plugin = plugin_class(task_config=task_config, task_function=_dummy_task_func)
        task_type = f"nb-{self._plugin.task_type}"
        self._notebook_path = notebook_path

        if not os.path.exists(self._notebook_path):
            raise ValueError(f"Illegal notebook path passed in {self._notebook_path}")

        super().__init__(name, task_config, task_type=task_type, interface=Interface(inputs=inputs, outputs=outputs),
                         **kwargs)

    @property
    def notebook_path(self) -> str:
        return self._notebook_path

    @property
    def output_notebook_path(self) -> str:
        return self._notebook_path.split(".ipynb")[0] + "-out.ipynb"

    @property
    def rendered_output_path(self) -> str:
        return self._notebook_path.split(".ipynb")[0] + "-out.html"

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        return self._plugin.pre_execute(user_params)

    @staticmethod
    def extract_outputs(nb: str) -> LiteralMap:
        """
         Parse Outputs from Notebook.
         This looks for a cell, with the tag "outputs" to be present.
        """
        with open(nb) as json_file:
            data = json.load(json_file)
            for p in data["cells"]:
                meta = p["metadata"]
                if "outputs" in meta["tags"]:
                    outputs = " ".join(p["outputs"][0]["data"]["text/plain"])
                    return LiteralMap.from_flyte_idl(_pb2_lm.ParseFromString(outputs))
        return None

    @staticmethod
    def render_nb_html(from_nb: str, to: str):
        """
            render output notebook to html
            We are using nbconvert htmlexporter and its classic template
            later about how to customize the exporter further.
        """
        html_exporter = HTMLExporter()
        html_exporter.template_name = 'classic'
        (body, resources) = html_exporter.from_notebook_node(from_nb)

        with open(to, "w+") as f:
            f.write(body)

    def execute(self, **kwargs) -> Any:
        logging.info(f"Hijacking the call for task-type {self.task_type}, to call notebook.")
        # Execute Notebook via Papermill.
        pm.execute_notebook(self._notebook_path, self.output_notebook_path, parameters=kwargs)

        outputs = self.extract_outputs(self.output_notebook_path)
        self.render_nb_html(self.output_notebook_path, self.rendered_output_path)
        for k, v in self.interface.outputs.items():
            if outputs
                [k]

    def post_execute(self, user_params: ExecutionParameters, rval: Any) -> Any:
        return self._plugin.post_execute(user_params, rval)


def record_outputs(**kwargs) -> str:
    """
     Use this method to record outputs from a notebook.
     It will convert all outputs to a Flyte understandable format. For Files, Directories, please use FlyteFile or
     FlyteDirectory, or wrap up your paths in these decorators.
     """
    if kwargs is None:
        return ""

    m = {}
    ctx = FlyteContext.current_context()
    for k, v in kwargs.items():
        expected = TypeEngine.to_literal_type(type(v))
        lit = TypeEngine.to_literal(ctx, python_type=type(v), python_val=v, expected=expected)
        m[k] = lit
    return LiteralMap(literals=m).to_flyte_idl().SerializeToString()
