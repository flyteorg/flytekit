import flytekit as fl
from flytekit import conditional
from flytekit.core.task import Echo

echo_radius = Echo(name="noop", inputs={"radius": float})


@fl.task
def calculate_circle_circumference(radius: float) -> float:
    return 2 * 3.14 * radius  # Task to calculate the circumference of a circle


@fl.task
def calculate_circle_area(radius: float) -> float:
    return 3.14 * radius * radius  # Task to calculate the area of a circle


@fl.task
def nop(radius: float) -> float:
    return radius  # Task that does nothing, effectively a no-op


@fl.workflow
def wf(radius: float = 0.5, get_area: bool = False, get_circumference: bool = True):
    echoed_radius = nop(radius=radius)
    (
        conditional("if_area")
        .if_(get_area.is_true())
        .then(calculate_circle_area(radius=radius))
        .else_()
        .then(echo_radius(echoed_radius))
    )
    (
        conditional("if_circumference")
        .if_(get_circumference.is_true())
        .then(calculate_circle_circumference(radius=echoed_radius))
        .else_()
        .then(echo_radius(echoed_radius))
    )
