# Flytekit python plugins
All flytekitplugins maintained by the core team are added here. It is not
necessary to add plugins here, but this is a good starting place.

## Currently available plugins


| Plugin                       | Installation                                         | Description                                                                                                                 | Version                                                                                                                                   | Type          |
|------------------------------|------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| AWS Sagemaker Training       | ```bash pip install flytekitplugins-awssagemaker ``` | Installs SDK to author Sagemaker built-in and custom training jobs in python                                                | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-awssagemaker/) | Backend       |
| Hive Queries                 | ```bash pip install flytekitplugins-hive ```         | Installs SDK to author Hive Queries that can be executed on a configured hive backend using Flyte backend plugin            | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-hive/)         | Backend       |
| K8s distributed PyTorch Jobs | ```bash pip install flytekitplugins-kfpytorch ```    | Installs SDK to author Distributed pyTorch Jobs in python using Kubeflow PyTorch Operator                                   | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-kfpytorch/)    | Backend       |
| K8s native tensorflow Jobs   | ```bash pip install flytekitplugins-kftensorflow ``` | Installs SDK to author Distributed tensorflow Jobs in python using Kubeflow Tensorflow Operator                             | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-kftensorflow/) | Backend       |
| Papermill based Tasks        | ```bash pip install flytekitplugins-papermill ```    | Execute entire notebooks as Flyte Tasks and pass inputs and outputs between them and python tasks                           | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-papermill/)    | Flytekit-only |
| Pod Tasks                    | ```bash pip install flytekitplugins-pod ```          | Installs SDK to author Pods in python. These pods can have multiple containers, use volumes and have non exiting side-cars  | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-pod/)          | Flytekit-only |
| spark                        | ```bash pip install flytekitplugins-spark ```        | Installs SDK to author Spark jobs that can be executed natively on Kubernetes with a supported backend Flyte plugin         | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-spark/)        | Backend       |
| AWS Athena Queries           | ```bash pip install flytekitplugins-athena ```       | Installs SDK to author queries executed on AWS Athena                                                                       | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-athena/)       | Backend       |
| DOLT                         | ```bash pip install flytekitplugins-dolt ```         | Read & write dolt data sets and use dolt tables as native types                                                             | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-dolt/)         | Flytekit-only |
| Pandera                      | ```bash pip install flytekitplugins-pandera ```      | Use Pandera schemas as native Flyte types, which enable data quality checks.                                                | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-pandera/)      | Flytekit-only |
| SQLAlchemy                   | ```bash pip install flytekitplugins-sqlalchemy ```   | Write queries for any database that supports SQLAlchemy                                                                     | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-sqlalchemy/)   | Flytekit-only |


## Have a Plugin Idea?
Please [file an issue](https://github.com/flyteorg/flyte/issues/new?assignees=&labels=untriaged%2Cplugins&template=backend-plugin-request.md&title=%5BPlugin%5D)

## Development
Flyte plugins are structured as micro-libs and can be authored in an
independent repository. The plugins maintained by the core team are maintained in this repository and provide a simple way of discovery.
When authoring plugins here are some tips

1. The folder name has to be `flytekit-*`. e.g. `flytekit-hive`. In case you want to group for a specific service then use `flytekit-aws-athena`.
2. Flytekit plugins uses a concept called [Namespace packages](https://packaging.python.org/guides/creating-and-discovering-plugins/#using-namespace-packages). Thus the package structure is very important. Use the following python package structure,
   ```
   flytekit-myplugin/
      - README.md
      - setup.py
      - flytekitplugins
          - myplugin
   ```
   *NOTE* the inner package `flytekitplugins` does not have an `__init__.py` file.
3. The published packages have to be named as `flytekitplugins-{}`, where {} is a unique identifier for the plugin.

TODO: 4. Each plugin should have a README.md, which describes how to install it, and has a simple example for it.

*[Refer to this Blog to understand the idea of microlibs](https://medium.com/@jherreras/python-microlibs-5be9461ad979)*

## Unit tests
Plugins should have their own unit tests
