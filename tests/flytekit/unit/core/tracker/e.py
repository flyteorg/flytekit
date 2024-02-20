from flytekit.core.python_function_task import PythonInstanceTask


class E(PythonInstanceTask):
    ...


e_instantiated = E(name="e-instantiated", task_config={})
