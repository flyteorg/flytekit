# Flytekit

Library for easily authoring, testing, deploying, and interacting with Flyte tasks, workflows, and launch plans.

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

### Spark Plugin Installation

If `@spark_task` is to be used, one should install the `spark` plugin.

```bash
pip install flytekit[spark]
```

### Schema Plugin Installation

If `Types.Schema()` is to be used for computations involving large dataframes, one should install the `schema` extension.

```bash
pip install flytekit[schema]
```

### Sidecar Plugin Installation

If `@sidecar_task` is to be used, one should install the `sidecar` plugin.

```bash
pip install flytekit[sidecar]
```

### Full Installation

To install all or multiple available plugins, one can specify them individually:

```bash
pip install flytekit[sidecar,spark,schema]
```

Or install them with the `all` directive.

```bash
pip install flytekit[all]
```

## Testing

Flytekit is Python 2.7+ compatible, so when feasible, it is recommended to test with both Python 2 and 3.

### Unit Testing

#### Setup (Do Once)
```bash
virtualenv ~/.virtualenvs/flytekit
source ~/.virtualenvs/flytekit/bin/activate
python -m pip install -r requirements.txt
python -m pip install -U .[all]
```

#### Execute
```bash
source ~/.virtualenvs/flytekit/bin/activate
python -m pytest tests/flytekit/unit
shellcheck **/*.sh
```
