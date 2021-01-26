# Flytekit

[![PyPI version fury.io](https://badge.fury.io/py/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI download day](https://img.shields.io/pypi/dd/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI download month](https://img.shields.io/pypi/dm/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI format](https://img.shields.io/pypi/format/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
[![PyPI implementation](https://img.shields.io/pypi/implementation/flytekit.svg)](https://pypi.python.org/pypi/flytekit/)
![Codecov](https://img.shields.io/codecov/c/github/lyft/flytekit?style=plastic)


Python Library for easily authoring, testing, deploying, and interacting with Flyte tasks, workflows, and launch plans. To understand more about flyte refer to,
 - [Flyte homepage](https://flyte.org)
 - [Flyte master repository](https://github.com/lyft/flyte)

## Installation

Flytekit is the core extensible library to author Flyte workflows and tasks and interact with Flyte Backend services. Flyte plugins can be installed separately. 

### Base Installation

```bash
pip install flytekit==0.16.0b1
```

### Simple getting started

```python

```

### Learn Flytekit by example using
TODO Add link here

### Plugins:
Refer to (plugins/README.md)[plugins/README.md] for a list of available
plugins. There may be plugins outside of this list, but this list is maintained
by the core maintainers.

## Development

### Recipes

```
$ make
Available recipes:
  setup        Install requirements
  fmt          Format code with black and isort
  lint         Run linters
  test         Run tests
  requirements Compile requirements
```

### Setup (Do Once)

```bash
virtualenv ~/.virtualenvs/flytekit
source ~/.virtualenvs/flytekit/bin/activate
make setup
```

### Formatting

We use [black](https://github.com/psf/black) and [isort](https://github.com/timothycrosley/isort) to autoformat code. Run the following command to execute the formatters:

```bash
source ~/.virtualenvs/flytekit/bin/activate
make fmt
```

### Testing

#### Unit Testing

```bash
source ~/.virtualenvs/flytekit/bin/activate
make test
```

### Updating requirements

Update requirements in [`requirements.in`](requirements.in) (or [`requirements-spark3.in`](requirements-spark3.in)), or update requirements for development in [`dev-requirements.in`](dev-requirements.in). Then, validate, pin and freeze all requirements by running:

```bash
source ~/.virtualenvs/flytekit/bin/activate
make requirements
```

This will re-create the [`requirements.txt`](requirements.txt) (or [`requirements-spark3.in`](requirements-spark3.in)) and [`dev-requirements.txt`](dev-requirements.txt) files which will be used for testing. You will have also have to re-run `make setup` to update your local environment with the updated requirements.
