"""
# Remote Access

This module provides utilities for performing operations on tasks, workflows, launchplans, and executions. For example, the following code fetches and executes a workflow:

```python
# create a remote object from flyte config and environment variables
FlyteRemote(config=Config.auto())
FlyteRemote(config=Config.auto(config_file=....))
FlyteRemote(config=Config(....))
```
Or if you need to specify a custom cert chain
(options and compression are also respected keyword arguments)
```python
FlyteRemote(private_key=your_private_key_bytes, root_certificates=..., certificate_chain=...)


### fetch a workflow from the flyte backend
remote = FlyteRemote(...)
flyte_workflow = remote.fetch_workflow(name="my_workflow", version="v1")

# execute the workflow, wait=True will return the execution object after it's completed
workflow_execution = remote.execute(flyte_workflow, inputs={"a": 1, "b": 10}, wait=True)

# inspect the execution's outputs
print(workflow_execution.outputs)
```

## Entrypoint

| Class |  | Description |
|-------|-------------|-------------|
| {{< py_class_ref remote.FlyteRemote >}} | {{< py_class_docsum remote.FlyteRemote >}} | The main class for interacting with a Flyte backend. |
| {{< py_class_ref remote.Config >}} | {{< py_class_docsum remote.Config >}} | Configuration options for the FlyteRemote client. |
| {{< py_class_ref remote.Options >}} | {{< py_class_docsum remote.Options >}} | Configuration options for the FlyteRemote client. |
| {{< py_class_ref remote.FlyteTask >}} | {{< py_class_docsum remote.FlyteTask >}} | Represents a registered Flyte task. |
| {{< py_class_ref remote.FlyteWorkflow >}} | {{< py_class_docsum remote.FlyteWorkflow >}} | Represents a registered Flyte workflow. |
| {{< py_class_ref remote.FlyteLaunchPlan >}} | {{< py_class_docsum remote.FlyteLaunchPlan >}} | Represents a registered Flyte launch plan. |
| {{< py_class_ref remote.FlyteNode >}} | {{< py_class_docsum remote.FlyteNode >}} | Base class for nodes in a Flyte workflow. |
| {{< py_class_ref remote.FlyteTaskNode >}} | {{< py_class_docsum remote.FlyteTaskNode >}} | Represents a task node in a Flyte workflow. |
| {{< py_class_ref remote.FlyteWorkflowNode >}} | {{< py_class_docsum remote.FlyteWorkflowNode >}} | Represents a subworkflow node in a Flyte workflow. |
| {{< py_class_ref remote.FlyteWorkflowExecution >}} | {{< py_class_docsum remote.FlyteWorkflowExecution >}} | Represents an execution of a Flyte workflow. |
| {{< py_class_ref remote.FlyteTaskExecution >}} | {{< py_class_docsum remote.FlyteTaskExecution >}} | Represents an execution of a Flyte task. |
| {{< py_class_ref remote.FlyteNodeExecution >}} | {{< py_class_docsum remote.FlyteNodeExecution >}} | Represents an execution of a node within a workflow. |
| {{< py_class_ref remote.FlyteBranchNode >}} | {{< py_class_docsum remote.FlyteBranchNode >}} | Represents a branch node in a Flyte workflow. |
"""

from flytekit.remote.entities import (
    FlyteBranchNode,
    FlyteLaunchPlan,
    FlyteNode,
    FlyteTask,
    FlyteTaskNode,
    FlyteWorkflow,
    FlyteWorkflowNode,
)
from flytekit.remote.executions import FlyteNodeExecution, FlyteTaskExecution, FlyteWorkflowExecution
from flytekit.remote.remote import FlyteRemote
