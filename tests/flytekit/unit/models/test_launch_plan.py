from __future__ import absolute_import

from flytekit.models import launch_plan, schedule, interface, types


def test_metadata():
    obj = launch_plan.LaunchPlanMetadata(schedule=None, notifications=[])
    obj2 = launch_plan.LaunchPlanMetadata.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2


def test_metadata_schedule():
    s = schedule.Schedule("asdf", "1 3 4 5 6 7")
    obj = launch_plan.LaunchPlanMetadata(schedule=s, notifications=[])
    assert obj.schedule == s
    obj2 = launch_plan.LaunchPlanMetadata.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.schedule == s


def test_lp_closure():
    v = interface.Variable(
        types.LiteralType(simple=types.SimpleType.BOOLEAN), "asdf asdf asdf"
    )
    p = interface.Parameter(var=v)
    parameter_map = interface.ParameterMap({"ppp": p})
    parameter_map.to_flyte_idl()
    variable_map = interface.VariableMap({"vvv": v})
    obj = launch_plan.LaunchPlanClosure(
        state=launch_plan.LaunchPlanState.ACTIVE,
        expected_inputs=parameter_map,
        expected_outputs=variable_map,
    )
    assert obj.expected_inputs == parameter_map
    assert obj.expected_outputs == variable_map

    obj2 = launch_plan.LaunchPlanClosure.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.expected_inputs == parameter_map
    assert obj2.expected_outputs == variable_map
