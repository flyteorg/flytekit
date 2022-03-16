from os import path as _path

from flyteidl.core import compiler_pb2 as _compiler_pb2
from flyteidl.admin import launch_plan_pb2, execution_pb2

from flytekit.models.core import compiler as compiler_model
from flytekit.models import launch_plan, execution

# Helper functions.
# Easier to have a separate function for each rather than remembering file names

basepath = _path.dirname(__file__)


def get_compiled_workflow_closure() -> compiler_model.CompiledWorkflowClosure:
    cwc_pb = _compiler_pb2.CompiledWorkflowClosure()
    # So that tests that use this work when run from any directory
    filepath = _path.abspath(_path.join(basepath, "resources/protos", "CompiledWorkflowClosure.pb"))
    with open(filepath, "rb") as fh:
        cwc_pb.ParseFromString(fh.read())

    return compiler_model.CompiledWorkflowClosure.from_flyte_idl(cwc_pb)


def get_launch_plan_spec() -> launch_plan.LaunchPlanSpec:
    lps = launch_plan_pb2.LaunchPlanSpec()
    filepath = _path.abspath(_path.join(basepath, "resources/protos", "parent_wf_lp.pb"))
    with open(filepath, "rb") as fh:
        lps.ParseFromString(fh.read())
    return launch_plan.LaunchPlanSpec.from_flyte_idl(lps)


def get_merge_sort_cwc() -> compiler_model.CompiledWorkflowClosure:
    cwc_pb = _compiler_pb2.CompiledWorkflowClosure()
    filepath = _path.abspath(_path.join(basepath, "resources/protos", "merge_sort_cwc.pb"))
    with open(filepath, "rb") as fh:
        cwc_pb.ParseFromString(fh.read())
    return compiler_model.CompiledWorkflowClosure.from_flyte_idl(cwc_pb)


def get_merge_sort_exec() -> execution.Execution:
    exec_pb = execution_pb2.Execution()
    filepath = _path.abspath(_path.join(basepath, "resources/protos", "merge_sort_ex.pb"))
    with open(filepath, "rb") as fh:
        exec_pb.ParseFromString(fh.read())
    return execution.Execution.from_flyte_idl(exec_pb)


def get_parent_wf_exec() -> execution.Execution:
    exec_pb = execution_pb2.Execution()
    filepath = _path.abspath(_path.join(basepath, "resources/protos", "parent_wf_ex.pb"))
    with open(filepath, "rb") as fh:
        exec_pb.ParseFromString(fh.read())
    return execution.Execution.from_flyte_idl(exec_pb)
