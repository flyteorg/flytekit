import flytekit.common.workflow_execution as workflow_execution
from flytekit.engines import loader as _engine_loader

sdk_workflow = workflow_execution.SdkWorkflowExecution.fetch("flytekit", "production", "neebqpndip")
outputs = _engine_loader.get_engine().get_workflow_execution(sdk_workflow).get_outputs()
print("outputs: {}".format(outputs))
