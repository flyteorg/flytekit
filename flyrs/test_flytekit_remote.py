import timeit
import matplotlib.pyplot as plt

from flytekit.configuration import Config
from flytekit.remote import FlyteRemote

PROJECT = "flytesnacks"
DOMAIN = "development"

remote_py = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN)
task_py = remote_py.fetch_task(
    project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw"
)
# print(task_py)

remote_rs = FlyteRemote(Config.auto(), enable_rs=True, default_project=PROJECT, default_domain=DOMAIN)
task_rs = remote_rs.fetch_task(
    project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw"
)
# print(task_rs)

print(task_py == task_rs)


setup = """
from flytekit.remote import FlyteRemote;
from flytekit.configuration import Config;
PROJECT = "flytesnacks";
DOMAIN = "development";
remote_py = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN);
remote_rs = FlyteRemote(Config.auto(), enable_rs=True, default_project=PROJECT, default_domain=DOMAIN);
"""

fetch_task_in_py = """task_py = remote_py.fetch_task(project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw")"""
fetch_task_in_rs = """task_rs = remote_rs.fetch_task(project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw")"""

r = 10
Xs = [1, 10, 100, 1000]
py_elpased, rs_elpased = [], []
for x in Xs:
    # Python gRPC
    py_elpased.append(sum(timeit.repeat(fetch_task_in_py, setup=setup, repeat=r, number=x))/r)
    print()
    # Rust gRPC
    rs_elpased.append(sum(timeit.repeat(fetch_task_in_rs, setup=setup, repeat=r, number=x))/r)
    print()
plt.xlabel('# of fetched tasks')
plt.ylabel('average elapsed time (s)')
plt.plot(Xs, py_elpased,'r-',label='Python gRPC')
plt.plot(Xs, rs_elpased,'b-',label='Rust gRPC')
plt.legend()
plt.savefig("perf.png")