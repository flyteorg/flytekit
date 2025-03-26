"""
# Remote Access

This module provides utilities for performing operations on tasks, workflows, launchplans, and executions. For example, the following code fetches and executes a workflow:

```python
# create a remote object from flyte config and environment variables
FlyteRemote(config=Config.auto())
FlyteRemote(config=Config.auto(config_file=....))
FlyteRemote(config=Config(....))
```

# Or if you need to specify a custom cert chain
# (options and compression are also respected keyword arguments)
FlyteRemote(private_key=your_private_key_bytes, root_certificates=..., certificate_chain=...)

# fetch a workflow from the flyte backend
remote = FlyteRemote(...)
flyte_workflow = remote.fetch_workflow(name="my_workflow", version="v1")

# execute the workflow, wait=True will return the execution object after it's completed
workflow_execution = remote.execute(flyte_workflow, inputs={"a": 1, "b": 10}, wait=True)

# inspect the execution's outputs
print(workflow_execution.outputs)
```

## Entrypoint

### FlyteRemote
The main class for interacting with a Flyte backend.

### Options
Configuration options for the FlyteRemote client.

## Entities

### FlyteTask
Represents a registered Flyte task.

### FlyteWorkflow
Represents a registered Flyte workflow.

### FlyteLaunchPlan
Represents a registered Flyte launch plan.

## Entity Components

### FlyteNode
Base class for nodes in a Flyte workflow.

### FlyteTaskNode
Represents a task node in a Flyte workflow.

### FlyteWorkflowNode
Represents a subworkflow node in a Flyte workflow.

## Execution Objects

### FlyteWorkflowExecution
Represents an execution of a Flyte workflow.

### FlyteTaskExecution
Represents an execution of a Flyte task.

### FlyteNodeExecution
Represents an execution of a node within a workflow.

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
