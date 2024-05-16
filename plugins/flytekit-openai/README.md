# OpenAI Plugins

The plugin currently features ChatGPT and Batch API agents.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-openai
```

## ChatGPT

The ChatGPT plugin allows you to run ChatGPT tasks within the Flyte workflow without requiring any code changes.

```python
from flytekit import task, workflow
from flytekitplugins.openai import ChatGPTTask, ChatGPTConfig

chatgpt_small_job = ChatGPTTask(
    name="chatgpt gpt-3.5-turbo",
    openai_organization="org-NayNG68kGnVXMJ8Ak4PMgQv7",
    chatgpt_config={
            "model": "gpt-3.5-turbo",
            "temperature": 0.7,
    },
)

chatgpt_big_job = ChatGPTTask(
    name="chatgpt gpt-4",
    openai_organization="org-NayNG68kGnVXMJ8Ak4PMgQv7",
    chatgpt_config={
            "model": "gpt-4",
            "temperature": 0.7,
    },
)


@workflow
def wf(message: str) -> str:
    message = chatgpt_small_job(message=message)
    message = chatgpt_big_job(message=message)
    return message


if __name__ == "__main__":
    print(wf(message="hi"))
```

## Batch API

The Batch API agent allows you to submit requests for asynchronous batch processing on OpenAI.
You can provide either a JSONL file or a JSON iterator, and the agent handles the upload to OpenAI,
creation of the batch, and downloading of the output and error files.

```python
from typing import Iterator

from flytekit import workflow, Secret
from flytekit.types.file import JSONLFile
from flytekit.types.iterator import JSON
from flytekitplugins.openai import create_batch, BatchResult


def jsons():
    for x in [
        {
            "custom_id": "request-1",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-3.5-turbo",
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "What is 2+2?"},
                ],
            },
        },
        {
            "custom_id": "request-2",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-3.5-turbo",
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "Who won the world series in 2020?"},
                ],
            },
        },
    ]:
        yield x


it_batch = create_batch(
    name="gpt-3.5-turbo",
    openai_organization="your-org",
    secret=Secret(group="openai-secret", key="api-key"),
)

file_batch = create_batch(
    name="gpt-3.5-turbo",
    openai_organization="your-org",
    secret=Secret(group="openai-secret", key="api-key"),
    is_json_iterator=False,
)


@workflow
def json_iterator_wf(json_vals: Iterator[JSON] = jsons()) -> BatchResult:
    return it_batch(jsonl_in=json_vals)


@workflow
def jsonl_wf(jsonl_file: JSONLFile = "data.jsonl") -> BatchResult:
    return file_batch(jsonl_in=jsonl_file)
```
