# Flytekit Envd Plugin

[envd](https://github.com/tensorchord/envd) is a command-line tool that helps you create the container-based development environment for AI/ML.

Development environments are full of python and system dependencies, CUDA, BASH scripts, Dockerfiles, SSH configurations, Kubernetes YAMLs, and many other clunky things that are always breaking. envd is to solve the problem:

With `flytekitplugins-envd`, people easily create a docker image for the workflow without writing a docker file.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-envd
```

Example
```python
# from flytekit import task, workflow
# 
# @task(image_spec=ImageSpec(packages=["pandas", "numpy"], registry="pingsutw"))
# def t1() -> str:
#     print("hello")
#     print("hello")
#     return "hello"
```
