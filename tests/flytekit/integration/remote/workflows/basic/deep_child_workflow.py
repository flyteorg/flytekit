from flytekit import LaunchPlan, task, workflow
from flytekit.models.common import Labels


@task
def double(a: int) -> int:
    return a * 2


@task
def add(a: int, b: int) -> int:
    return a + b


@workflow
def my_deep_childwf(a: int = 42) -> int:
    b = double(a=a)
    return b

deep_child_lp = LaunchPlan.get_or_create(my_deep_childwf, name="my_fixed_deep_child_lp", labels=Labels({"l1": "v1"}))


@workflow
def my_childwf(a: int = 42) -> int:
    b = deep_child_lp(a=a)
    c = double(a=b)
    return c


shallow_child_lp = LaunchPlan.get_or_create(my_childwf, name="my_shallow_fixed_child_lp", labels=Labels({"l1": "v1"}))


@workflow
def parent_wf(a: int) -> int:
    x = double(a=a)
    y = shallow_child_lp(a=x)
    z = add(a=x, b=y)
    return z


if __name__ == "__main__":
    print(f"Running parent_wf(a=3) {parent_wf(a=3)}")
