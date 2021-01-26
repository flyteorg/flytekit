# Flytekit python plugins
All flytekitplugins maintained by the core team are added here. It is not
necessary to add plugins here, but this is a good starting place.

## Currently available plugins


| Plugin                       | Installation                                         | Description                                                                                                                 | Version                                                                                                                                   |
|------------------------------|------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| AWS Sagemaker Training       | ```bash pip install flytekitplugins-awssagemaker ``` | Installs SDK to author Sagemaker built-in and custom training jobs in python                                                | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-awssagemaker/) |
| Hive Queries                 | ```bash pip install flytekitplugins-hive ```         | Installs SDK to author Hive Queries that can be executed on a configured hive backend using Flyte backend plugin            | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-hive/)         |
| K8s distributed PyTorch Jobs | ```bash pip install flytekitplugins-kfpytorch ```    | Installs SDK to author Distributed pyTorch Jobs in python using Kubeflow PyTorch Operator                                   | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-kfpytorch/)    |
| K8s native tensorflow Jobs   | ```bash pip install flytekitplugins-kftensorflow ``` | Installs SDK to author Distributed tensorflow Jobs in python using Kubeflow Tensorflow Operator                             | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-kftensorflow/) |
| Papermill based Tasks        | ```bash pip install flytekitplugins-papermill ```    | Execute entire notebooks as Flyte Tasks and pass inputs and outputs between them and python tasks                           | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-papermill/)    |
| Pod Tasks                    | ```bash pip install flytekitplugins-pod ```          | Installs SDK to author Pods in python. These pods can have multiple containers, use volumes and have non exiting side-cars  | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-pod/)          |
| spark                        | ```bash pip install flytekitplugins-spark ```        | Installs SDK to author Spark jobs that can be executed natively on Kubernetes with a supported backend Flyte plugin         | [![PyPI version fury.io](https://badge.fury.io/py/flytekitplugins-spark.svg)](https://pypi.python.org/pypi/flytekitplugins-spark/)        |


## Have a Plugin Idea?
Please file an issue and or create a PR. 

## Development
Flyte plugins are structured as micro-libs and can be authored in an
independent repository. The plugins maintained by the core team are maintained
in this repository

## (Refer to this Blog to understand the idea of microlibs)[https://medium.com/@jherreras/python-microlibs-5be9461ad979]

## Conventions
All plugins should expose a library in the format **flytekitplugins-{}**, where
{} is a unique identifier for the plugin.

## Unit tests
Plugins should have their own unit tests
