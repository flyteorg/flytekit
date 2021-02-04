from typing import Any, Optional

from flytekit.core.base_task import PythonTask, TaskMetadata
from flytekit.core.interface import transform_interface_to_list_interface


class MapPythonTask(PythonTask):
    """
    TODO We might need a special entrypoint to start execution of this task type as there is possibly no instance of this
    type and it needs to be dynamically generated at runtime. We can easily generate it by passing it the actual task
    that is to be generated.

    To do this we might have to give up on supporting lambda functions initially
    """

    def __init__(self, tk: PythonTask, metadata: Optional[TaskMetadata] = None, **kwargs):
        collection_interface = transform_interface_to_list_interface(tk.python_interface)
        name = "mapper_" + tk.name
        self._run_task = tk
        super().__init__(
            name=name,
            interface=collection_interface,
            metadata=metadata,
            task_type="map_task",
            task_config=None,
            **kwargs,
        )

    @property
    def run_task(self) -> PythonTask:
        return self._run_task

    def execute(self, **kwargs) -> Any:
        all_types_are_collection = True
        any_key = None
        for k, v in self._run_task.interface.inputs.items():
            any_key = k
            if v.type.collection_type is None:
                all_types_are_collection = False
                break

        # If all types are collection we can just handle the call as a pass through
        if all_types_are_collection:
            return self._run_task.execute(**kwargs)

        # If all types are not collection then we need to perform batching
        batch = {}

        outputs_expected = True
        if not self.interface.outputs:
            outputs_expected = False
        outputs = []
        for k in self.interface.outputs.keys():
            outputs.append([])
        for i in range(len(kwargs[any_key])):
            for k in self.interface.inputs.keys():
                batch[k] = kwargs[k][i]
            o = self._run_task.execute(**batch)
            if outputs_expected:
                for x in range(len(outputs)):
                    outputs[x].append(o[x])
        if len(outputs) == 1:
            return outputs[0]

        return tuple(outputs)


def maptask(tk: PythonTask, concurrency="auto", metadata=None):
    if not isinstance(tk, PythonTask):
        raise ValueError(f"Only Flyte Task types are supported in maptask currently, received {type(tk)}")
    # We could register in a global singleton here?
    return MapPythonTask(tk, metadata=metadata)
