import random
from flytekit import task, workflow


@task 
def add_rand(n: int) -> float:
    return float(n + random.randint(-1000, 1000))

@task
def bad_types(a: int) -> float:
    return str(a)

@task
def wf(a: int, b: int):
    bad_types(a=add_rand(n=a))
    

if __name__ == "__main__":
    print(wf(a=1, b=1))
    # print(bad_types(a=1))
    # print(bad_types(a=str(1)))
