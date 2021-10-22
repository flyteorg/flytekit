<html>
    <p align="center">
        <img src="https://github.com/flyteorg/flyte/blob/master/rsts/images/flyte_circle_gradient_1_4x4.png" alt="Flyte Logo" width="100">
    </p>
    <h1 align="center">
        Flytekit Python
    </h1>
    <p align="center">
        Flytekit Python is the Python SDK built on top of Flyte
    </p>
    <h3 align="center">
        <a href="plugins/README.md">Plugins</a>
        <span> · </span>
        <a href="https://docs.flyte.org/projects/flytekit/en/latest/contributing.html">Contribution Guide</a>
    </h3>
</html>

[![PyPI version fury.io](https://badge.fury.io/py/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI download day](https://img.shields.io/pypi/dd/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI download month](https://img.shields.io/pypi/dm/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI format](https://img.shields.io/pypi/format/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI implementation](https://img.shields.io/pypi/implementation/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
![Codecov](https://img.shields.io/codecov/c/github/flyteorg/flytekit?style=plastic)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![Docs](https://readthedocs.org/projects/flytekit/badge/?version=latest&style=plastic)](https://flytekit.rtfd.io)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Slack](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://slack.flyte.org)

Flytekit Python is the Python Library for easily authoring, testing, deploying, and interacting with Flyte tasks, workflows, and launch plans.

If you haven't explored Flyte yet, please refer to:
 - [Flyte homepage](https://flyte.org)
 - [Flyte core repository](https://github.com/flyteorg/flyte)

## 🚀 Quick Start

Flytekit is the core extensible library to author Flyte workflows and tasks and interact with Flyte backend services.

### Installation

```bash
pip install flytekit
```

### A Simple Example

```python
from flytekit import task, workflow

@task(cache=True, cache_version="1", retries=3)
def sum(x: int, y: int) -> int:
    return x + y

@task(cache=True, cache_version="1", retries=3)
def square(z: int) -> int:
    return z*z

@workflow
def my_workflow(x: int, y: int) -> int:
    return sum(x=square(z=x), y=square(z=y))
```

## 📦 Resources
- [Learn Flytekit by examples](https://flytecookbook.readthedocs.io/)
- [Flytekit API documentation](https://flytekit.readthedocs.io/)


## 📖 How to Contribute to Flytekit
You can find the detailed contribution guide [here](https://docs.flyte.org/projects/flytekit/en/latest/contributing.html). Plugins' contribution guide is included as well.

## Code Guide
The first version of the flytekit library was written circa 2017, before mypy typing was mainstream, and
targeted Python 2. That legacy code will be fully deprecated and removed in 2022 but because there are still
users of flytekit that rely on that legacy api, you'll see 2 separate and distinct code paths within this repo.
Users and contributors should ignore the legacy sections. Below is a listing of the most important packages that
comprise the new API:

- `flytekit/core`
  This holds all the core functionality of the new API.
- `flytekit/types`
  We bundle some special types like `FlyteFile, FlyteSchema etc` by default here.
- `flytekit/extend`
  This is the future home of extension points, and currently serves as the raw documentation for extensions.
- `flytekit/extras`
  This contains code that we want bundled with flytekit but not everyone may find useful (for example AWS and GCP
  specific logic).
- `flytekit/remote`
  This implements the interface to interact with the Flyte service. Think of the code here as the Python-object version of Console.
- `flytekit/testing`
  is the future home for testing functionality like `mock` etc, and currently serves as documentation.
  All test extensions should be imported from here.
- `flytekit/models`
  Protobuf generated Python code is not terribly user-friendly, so we improve upon those `flyteidl` classes here.
- `plugins`
  is the source of all plugins
- `flytekit/bin/entrypoint.py`
  The run time entrypoint for flytekit. When a task kicks off, this is where the click command goes.
- `flytekit/clis`
  This is the home for the clis.
- `flytekit/configuration`
  This holds all the configuration objects, but dependency on configuration should be carefully considered as it
  makes compiled Flyte tasks and workflows less portable (i.e. if you run `pyflyte package` can someone else use
  those serialized objects).

Most of the other folders are for legacy Flytekit, support for which will be dropped in early 2022. For the most part,
please ignore the following folders:

- `flytekit/plugins`
- `flytekit/common`
  (the `translator.py` file is an exception)
- `flytekit/engines`
- `flytekit/interfaces`
- `flytekit/sdk`
- `flytekit/type_engines`

Please also see the [overview section](https://docs.flyte.org/projects/flytekit/en/latest/design/index.html) of the formal flytekit documentation for more information.

## 🐞 File an Issue
Refer to the [issues](https://docs.flyte.org/en/latest/community/contribute.html#issues) section in the contribution guide if you'd like to file an issue.

## 🔌 Flytekit Plugins
Refer to [plugins/README.md](plugins/README.md) for a list of available plugins.
There may be plugins outside of this list, but this list is maintained by the core maintainers.
