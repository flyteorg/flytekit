# Flytekit VSCode Plugin

The Flytekit VSCode plugin offers an easy solution for users to run tasks within an interactive VSCode server, compatible with any image. `@vscode` is a decorator which users can put within @task and user function. With `@vscode`, the task will install vscode dependencies and run a vscode server instead of the user defined functions.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-vscode
```

## Task Example
```python
from flytekit import task
from flytekitplugins.vscode import vscode

@task
@vscode
def train():
    ...
```

## User Guide
1. Run the decorated task on the remote. For example: `pyflyte run --remote [PYTHONFILE] [WORKFLOW|TASK] [ARGS]...`
2. Once the code server is prepared, you can forward a local port to the pod. For example: `kubectl port-forward -n [NAMESPACE] [PODNAME] 8080:8080`.
3. You can access the server by opening a web browser and navigating to `localhost:8080`.

VSCode example screenshot:
<img src="https://raw.githubusercontent.com/flyteorg/flytekit/master/plugins/flytekit-vscode/example.png">

## Build Custom Image with VSCode Plugin
Users have the option to create a custom image by including the following lines in their Dockerfile.
```
# Include this line if the image does not already have 'curl' installed.
+ RUN apt-get -y install curl
# Download and extract the binary, and ensure it's added to the system's $PATH.
+ RUN mkdir /tmp/code-server
+ RUN curl -kfL -o /tmp/code-server/code-server-4.18.0-linux-amd64.tar.gz https://github.com/coder/code-server/releases/download/v4.18.0/code-server-4.18.0-linux-amd64.tar.gz
+ RUN tar -xzf /tmp/code-server/code-server-4.18.0-linux-amd64.tar.gz -C /tmp/code-server/
+ ENV PATH="/tmp/code-server/code-server-4.18.0-linux-amd64/bin:${PATH}"
```
