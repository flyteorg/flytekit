# Flytekit Perian Job Platform Plugin

Flyte Agent plugin for executing Flyte tasks on Perian Job Platform (perian.io).

Perian Job Platform is still in closed beta. Contact support@perian.io if you are interested in trying it out.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-perian
```

## Getting Started

This plugin allows executing `PythonFunctionTask` on Perian.

An [ImageSpec](https://docs.flyte.org/en/latest/user_guide/customizing_dependencies/imagespec.html) need to be built with the perian agent plugin installed.

### Parameters

The following parameters can be used to set the requirements for the Perian task. If any of the requirements are skipped, it is replaced with the cheapest option. At least one requirement value should be set.
* `cores`: Number of CPU cores
* `memory`: Amount of memory in GB
* `accelerators`: Number of accelerators
* `accelerator_type`: Type of accelerator (e.g. 'A100'). For a full list of supported accelerators, use the perian CLI list-accelerators command.
* `country_code`: Country code to run the job in (e.g. 'DE')

### Credentials

The following [secrets](https://docs.flyte.org/en/latest/user_guide/productionizing/secrets.html) are required to be defined for the agent server:
* Perian credentials:
    * `perian_organization`
    * `perian_token`
* AWS credentials for accessing the Flyte storage bucket. These credentials are never logged and are stored only until it is used, then immediately deleted:
    * `aws_access_key_id`
    * `aws_secret_access_key`
* (Optional) Custom docker registry for pulling the Flyte image:
    * `docker_registry_url`
    * `docker_registry_username`
    * `docker_registry_password`
