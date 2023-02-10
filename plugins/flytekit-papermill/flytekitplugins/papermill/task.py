import json
import os
import typing
from typing import Any

import nbformat
import papermill as pm
from flyteidl.core.literals_pb2 import LiteralMap as _pb2_LiteralMap
from google.protobuf import text_format as _text_format
from nbconvert import HTMLExporter

from flytekit import FlyteContext, PythonInstanceTask
from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import ExecutionParameters
from flytekit.deck.deck import Deck
from flytekit.extend import Interface, TaskPlugins, TypeEngine
from flytekit.loggers import logger
from flytekit.models import task as task_models
from flytekit.models.literals import LiteralMap
from flytekit.types.file import HTMLPage, PythonNotebook

T = typing.TypeVar("T")


def _dummy_task_func():
    return None


PAPERMILL_TASK_PREFIX = "pm.nb"


class NotebookTask(PythonInstanceTask[T]):
    """
    Simple Papermill based input output handling for a Python Jupyter notebook. This task should be used to wrap
    a Notebook that has 2 properties
    Property 1:
    One of the cells (usually the first) should be marked as the parameters cell. This task will inject inputs after this
    cell. The task will inject the outputs observed from Flyte

    Property 2:
    For a notebook that produces outputs, that should be consumed by a subsequent notebook, use the method
    :py:func:`record_outputs` in your notebook after the outputs are ready and pass all outputs.

    Usage:

    .. code-block:: python

        val_x = 10
        val_y = "hello"

        ...
        # cell begin
        from flytekitplugins.papermill import record_outputs

        record_outputs(x=val_x, y=val_y)
        #cell end

    Step 2: Wrap in a task
    Now point to the notebook and create an instance of :py:class:`NotebookTask` as follows

    Usage:

    .. code-block:: python

        nb = NotebookTask(
            name="modulename.my_notebook_task", # the name should be unique within all your tasks, usually it is a good
                                               # idea to use the modulename
            notebook_path="../path/to/my_notebook",
            render_deck=True,
            inputs=kwtypes(v=int),
            outputs=kwtypes(x=int, y=str),
            metadata=TaskMetadata(retries=3, cache=True, cache_version="1.0"),
        )

    Step 3: Task can be executed as usual

    Outputs
    -------
    The Task produces 2 implicit outputs.

    #. It captures the executed notebook in its entirety and is available from Flyte with the name ``out_nb``.
    #. It also converts the captured notebook into an ``html`` page, which the FlyteConsole will render called -
       ``out_rendered_nb``. If ``render_deck=True`` is passed, this html content will be inserted into a deck.

    .. note:

        Users can access these notebooks after execution of the task locally or from remote servers.

    .. todo:

        Implicit extraction of SparkConfiguration from the notebook is not supported.

    .. todo:

        Support for remote notebook execution, we can create a custom metadata field that is read by a propeller plugin
        or just passed down back into the container, so no need to rebuild the container.

    .. note:

        Some input types are not permitted by papermill. Types that cannot be passed directly into the cell are not
        supported - Only supported types are
        str, int, float, bool
        Most output types are supported as long as FlyteFile etc is used.

    """

    _IMPLICIT_OP_NOTEBOOK = "out_nb"
    _IMPLICIT_OP_NOTEBOOK_TYPE = PythonNotebook
    _IMPLICIT_RENDERED_NOTEBOOK = "out_rendered_nb"
    _IMPLICIT_RENDERED_NOTEBOOK_TYPE = HTMLPage

    def __init__(
        self,
        name: str,
        notebook_path: str,
        render_deck: bool = False,
        task_config: T = None,
        inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        outputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        **kwargs,
    ):
        # Each instance of NotebookTask instantiates an underlying task with a dummy function that will only be used
        # to run pre- and post- execute functions using the corresponding task plugin.
        # We rename the function name here to ensure the generated task has a unique name and avoid duplicate task name
        # errors.
        # This seem like a hack. We should use a plugin_class that doesn't require a fake-function to make work.
        plugin_class = TaskPlugins.find_pythontask_plugin(type(task_config))
        self._config_task_instance = plugin_class(task_config=task_config, task_function=_dummy_task_func, **kwargs)
        # Rename the internal task so that there are no conflicts at serialization time. Technically these internal
        # tasks should not be serialized at all, but we don't currently have a mechanism for skipping Flyte entities
        # at serialization time.
        self._config_task_instance._name = f"{PAPERMILL_TASK_PREFIX}.{name}"
        task_type = f"{self._config_task_instance.task_type}"
        task_type_version = self._config_task_instance.task_type_version
        self._notebook_path = os.path.abspath(notebook_path)

        self._render_deck = render_deck

        if not os.path.exists(self._notebook_path):
            raise ValueError(f"Illegal notebook path passed in {self._notebook_path}")

        if outputs:
            outputs.update(
                {
                    self._IMPLICIT_OP_NOTEBOOK: self._IMPLICIT_OP_NOTEBOOK_TYPE,
                    self._IMPLICIT_RENDERED_NOTEBOOK: self._IMPLICIT_RENDERED_NOTEBOOK_TYPE,
                }
            )
        super().__init__(
            name,
            task_config,
            task_type=task_type,
            task_type_version=task_type_version,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    @property
    def notebook_path(self) -> str:
        return self._notebook_path

    @property
    def output_notebook_path(self) -> str:
        return self._notebook_path.split(".ipynb")[0] + "-out.ipynb"

    @property
    def rendered_output_path(self) -> str:
        return self._notebook_path.split(".ipynb")[0] + "-out.html"

    def get_container(self, settings: SerializationSettings) -> task_models.Container:
        # The task name in the original command is incorrect because we use _dummy_task_func to construct the _config_task_instance.
        # Therefore, Here we replace the original command with NotebookTask's command.
        def fn(settings: SerializationSettings) -> typing.List[str]:
            return self.get_command(settings)

        self._config_task_instance.set_command_fn(fn)
        return self._config_task_instance.get_container(settings)

    def get_k8s_pod(self, settings: SerializationSettings) -> task_models.K8sPod:
        # The task name in original command is incorrect because we use _dummy_task_func to construct the _config_task_instance.
        # Therefore, Here we replace primary container's command with NotebookTask's command.
        def fn(settings: SerializationSettings) -> typing.List[str]:
            return self.get_command(settings)

        self._config_task_instance.set_command_fn(fn)
        return self._config_task_instance.get_k8s_pod(settings)

    def get_config(self, settings: SerializationSettings) -> typing.Dict[str, str]:
        return {**super().get_config(settings), **self._config_task_instance.get_config(settings)}

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        return self._config_task_instance.pre_execute(user_params)

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
                    # Sometimes log messages will be in the list of outputs, so iterate to find where
                    # the data is.
                    for record in p["outputs"]:
                        if "data" in record:
                            outputs = " ".join(record["data"]["text/plain"])
                            m = _pb2_LiteralMap()
                            _text_format.Parse(outputs, m)
                            return LiteralMap.from_flyte_idl(m)
        return None

    @staticmethod
    def render_nb_html(from_nb: str, to: str):
        """
        render output notebook to html
        We are using nbconvert htmlexporter and its classic template
        later about how to customize the exporter further.
        """
        html_exporter = HTMLExporter()
        html_exporter.template_name = "classic"
        nb = nbformat.read(from_nb, as_version=4)
        (body, resources) = html_exporter.from_notebook_node(nb)

        with open(to, "w+") as f:
            f.write(body)

    def execute(self, **kwargs) -> Any:
        """
        TODO: Figure out how to share FlyteContext ExecutionParameters with the notebook kernel (as notebook kernel
             is executed in a separate python process)
        For Spark, the notebooks today need to use the new_session or just getOrCreate session and get a handle to the
        singleton
        """
        logger.info(f"Hijacking the call for task-type {self.task_type}, to call notebook.")
        # Execute Notebook via Papermill.
        pm.execute_notebook(self._notebook_path, self.output_notebook_path, parameters=kwargs)  # type: ignore

        outputs = self.extract_outputs(self.output_notebook_path)
        self.render_nb_html(self.output_notebook_path, self.rendered_output_path)

        m = {}
        if outputs:
            m = outputs.literals
        output_list = []
        for k, type_v in self.python_interface.outputs.items():
            if k == self._IMPLICIT_OP_NOTEBOOK:
                output_list.append(self.output_notebook_path)
            elif k == self._IMPLICIT_RENDERED_NOTEBOOK:
                output_list.append(self.rendered_output_path)
            elif k in m:
                v = TypeEngine.to_python_value(ctx=FlyteContext.current_context(), lv=m[k], expected_python_type=type_v)
                output_list.append(v)
            else:
                raise RuntimeError(f"Expected output {k} of type {v} not found in the notebook outputs")

        return tuple(output_list)

    def post_execute(self, user_params: ExecutionParameters, rval: Any) -> Any:
        if self._render_deck:
            nb_deck = Deck(self._IMPLICIT_RENDERED_NOTEBOOK)
            with open(self.rendered_output_path, "r") as f:
                notebook_html = f.read()
            nb_deck.append(notebook_html)
            # Since user_params is passed by reference, this modifies the object in the outside scope
            # which then causes the deck to be rendered later during the dispatch_execute function.
            user_params.decks.append(nb_deck)

        return self._config_task_instance.post_execute(user_params, rval)


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
    return LiteralMap(literals=m).to_flyte_idl()
