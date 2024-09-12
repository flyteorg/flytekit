# import numpy as np
# import pandas as pd


from flytekit import task, workflow


@task
def say_hello() -> str:
    import numpy as np
    import pandas as pd

    from something_to_import import my_function

    # something dumb again
    my_function()
    ds = pd.DataFrame()
    a = np.array([1, 2, 3])  # lol
    print(ds, a)
    return "hello"


@workflow
def wf():
    say_hello()


if __name__ == "__main__":
    wf()
