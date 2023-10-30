# Flytekit VSCode Plugin

Flytekit VSCode plugin provides an simple way for users to run the task as a interactive vscode server with any image. `@vscode` is a decorator which users can put within @task and user function. With `@vscode`, the task will install vscode dependencies and run a vscode server instead of the user defined functions.

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
