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

Flytekit is designed for minimal footprint, and thus some features must be installed as extras.

### Base Installation

This is the lightest-weight SDK install. This installation includes everything you need to interact with Flyte.

Modules include:
1. The full Flyte IDL and an additional model layer for easier extension of the data model.
2. gRPC client for communicating with the platform.
3. Implementations for authoring and extending all Flyte entities (including tasks, workflows, and launch plans).

Tools include:
1. flyte-cli (Command-Line Interface for Interacting with the Flyte Platform)
2. pyflyte (Command-Line tool for easing the registration of Flyte entities)

```bash
pip install flytekit
```

### Plugin Installation
#### Spark

If `@spark_task` is to be used, one should install the `spark` plugin.

```bash
pip install "flytekit[spark]" for Spark 2.4.x
pip install "flytekit[spark3]" for Spark 3.x
```

Please note that Spark 2.4 support is deprecated and will be removed in a future release.

#### Schema 

If `Types.Schema()` is to be used for computations involving large dataframes, one should install the `schema` extension.

```bash
pip install "flytekit[schema]"
```

#### Sidecar

If `@sidecar_task` is to be used, one should install the `sidecar` plugin.

```bash
pip install "flytekit[sidecar]"
```

### Pytorch

If `@pytorch_task` is to be used, one should install the `pytorch` plugin.

```bash
pip install "flytekit[pytorch]"
```

### TensorFlow

If `@tensorflow_task` is to be used, one should install the `tensorflow` plugin.

```bash
pip install flytekit[tensorflow]
```

### Full Installation

To install all or multiple available plugins, one can specify them individually:

```bash
pip install "flytekit[sidecar,spark3,schema]"
```

Or install them with the `all` or `all-spark2.4` or `all-spark3` directives which will install all the plugins and a specific Spark version.
 Please note that `all` defaults to Spark 3.0 and Spark 2.4 support will be fully removed in a future release.


```bash
pip install "flytekit[all]"
```

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
