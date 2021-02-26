# Flytekit

[![PyPI version fury.io](https://badge.fury.io/py/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI download day](https://img.shields.io/pypi/dd/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI download month](https://img.shields.io/pypi/dm/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI format](https://img.shields.io/pypi/format/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI implementation](https://img.shields.io/pypi/implementation/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
![Codecov](https://img.shields.io/codecov/c/github/flyteorg/flytekit?style=plastic)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![Docs](https://readthedocs.org/projects/flytekit/badge/?version=latest&style=plastic)](https://flytekit.rtfd.io)


Python Library for easily authoring, testing, deploying, and interacting with Flyte tasks, workflows, and launch plans. To understand more about Flyte please refer to,
 - [Flyte homepage](https://flyte.org)
 - [Flyte core repository](https://github.com/flyteorg/flyte)

## Installation

Flytekit is the core extensible library to author Flyte workflows and tasks and interact with Flyte Backend services. Flyte plugins can be installed separately. 

### Base Installation

```bash
pip install flytekit==0.16.0b7
```

### Simple getting started

```python
from flytekit import task, workflow

@task(cache=True, cache_version="1", retries=3)
def sum(x: int, y: int) -> int:
    return x + y

@task(cache=True, cache_version="1", retries=3)
def square(x: int) -> int:
    return x*x

@workflow
def my_workflow(x: int, y: int) -> int:
    return sum(x=square(x=x),y=square(y=y))
```

### Learn Flytekit by example using
- [Learn flytekit by examples](https://flytecookbook.readthedocs.io/)
- [Flytekit API documentation](http://flytekit.readthedocs.io/)
- [Flyte documentation Hub](http://flytekit.readthedocs.io/)

### Contributions and Issues
Please see the [contributor guide](https://docs.flyte.org/projects/flytekit/en/latest/contributing.html) and file issues against the main [Flyte repo](https://github.com/flyteorg/flyte/issues).

### Plugins:
Refer to [plugins/README.md](plugins/README.md) for a list of available
plugins. There may be plugins outside of this list, but this list is maintained
by the core maintainers.
