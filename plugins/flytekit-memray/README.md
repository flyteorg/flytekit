# Memray Profiling Plugin

Memray tracks and reports memory allocations, both in python code and in compiled extension modules.
This Memray Profiling plugin enables memory tracking on the Flyte task level and renders a memgraph profiling graph on Flyte Deck.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-memray
```

Example
```python
from flytekit import workflow, task, ImageSpec
from flytekitplugins.memray import memray_profiling
import time


image = ImageSpec(
    name="memray_demo",
    packages=["flytekitplugins_memray"],
    registry="<your_cr_registry>",
)


def generate_data(n: int):
    leak_list = []
    for _ in range(n):  # Arbitrary large number for demonstration
        large_data = " " * 10**6  # 1 MB string
        leak_list.append(large_data)  # Keeps appending without releasing
        time.sleep(0.1)  # Slow down the loop to observe memory changes


@task(container_image=image, enable_deck=True)
@memray_profiling(memray_html_reporter="table")
def memory_usage(n: int) -> str:
    generate_data(n=n)

    return "Well"


@task(container_image=image, enable_deck=True)
@memray_profiling(trace_python_allocators=True, memray_reporter_args=["--leaks"])
def memory_leakage(n: int) -> str:
    generate_data(n=n)

    return "Well"


@workflow
def wf(n: int = 500):
    memory_usage(n=n)
    memory_leakage(n=n)
```
