# Flytekit Envd Plugin

[envd](https://github.com/tensorchord/envd) is a command-line tool that helps you create the container-based development environment for AI/ML.

Environments built with envd provide the following features out-of-the-box:
- Knowledge reuse in your team
- BuiltKit native, build up to 6x faster
- Smaller and leaner images

With `flytekitplugins-envd`, people easily create a docker image for the workflows without writing a docker file.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-envd
```

Example
```python
# from flytekit import task
# from flytekit.image_spec import ImageSpec
#
# @task(image_spec=ImageSpec(packages=["pandas", "numpy"], apt_packages=["git"], registry="flyteorg"))
# def t1() -> str:
#     return "hello"
```
