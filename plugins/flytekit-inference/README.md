# Inference Plugins

Serve models natively in Flyte tasks using inference providers like NIM, Ollama, and others.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-inference
```

## NIM

The NIM plugin allows you to serve optimized model containers that can include
NVIDIA CUDA software, NVIDIA Triton Inference SErver and NVIDIA TensorRT-LLM software.

```python
from flytekit import ImageSpec, Secret, task, Resources
from flytekitplugins.inference import NIM, NIMSecrets
from flytekit.extras.accelerators import A10G
from openai import OpenAI


image = ImageSpec(
    name="nim",
    registry="...",
    packages=["flytekitplugins-inference"],
)

nim_instance = NIM(
    image="nvcr.io/nim/meta/llama3-8b-instruct:1.0.0",
    secrets=NIMSecrets(
        ngc_image_secret="nvcrio-cred",
        ngc_secret_key=NGC_KEY,
        secrets_prefix="_FSEC_",
    ),
)


@task(
    container_image=image,
    pod_template=nim_instance.pod_template,
    accelerator=A10G,
    secret_requests=[
        Secret(
            key="ngc_api_key", mount_requirement=Secret.MountType.ENV_VAR
        )  # must be mounted as an env var
    ],
    requests=Resources(gpu="0"),
)
def model_serving() -> str:
    client = OpenAI(
        base_url=f"{nim_instance.base_url}/v1", api_key="nim"
    )  # api key required but ignored

    completion = client.chat.completions.create(
        model="meta/llama3-8b-instruct",
        messages=[
            {
                "role": "user",
                "content": "Write a limerick about the wonders of GPU computing.",
            }
        ],
        temperature=0.5,
        top_p=1,
        max_tokens=1024,
    )

    return completion.choices[0].message.content
```

## Ollama

The Ollama plugin allows you to serve LLMs locally.
You can either pull an existing model or create a new one.

```python
from textwrap import dedent

from flytekit import ImageSpec, Resources, task, workflow
from flytekitplugins.inference import Ollama, Model
from flytekit.extras.accelerators import A10G
from openai import OpenAI


image = ImageSpec(
    name="ollama_serve",
    registry="...",
    packages=["flytekitplugins-inference"],
)

ollama_instance = Ollama(
    model=Model(
        name="llama3-mario",
        modelfile=dedent("""\
        FROM llama3
        ADAPTER {inputs.gguf}
        PARAMETER temperature 1
        PARAMETER num_ctx 4096
        SYSTEM You are Mario from super mario bros, acting as an assistant.\
        """),
    )
)


@task(
    container_image=image,
    pod_template=ollama_instance.pod_template,
    accelerator=A10G,
    requests=Resources(gpu="0"),
)
def model_serving(questions: list[str], gguf: FlyteFile) -> list[str]:
    responses = []
    client = OpenAI(
        base_url=f"{ollama_instance.base_url}/v1", api_key="ollama"
    )  # api key required but ignored

    for question in questions:
        completion = client.chat.completions.create(
            model="llama3-mario",
            messages=[
                {"role": "user", "content": question},
            ],
            max_tokens=256,
        )
        responses.append(completion.choices[0].message.content)

    return responses
```

## vLLM

The vLLM plugin allows you to serve an LLM hosted on HuggingFace.

```python
import flytekit as fl
from openai import OpenAI

model_name = "google/gemma-2b-it"
hf_token_key = "vllm_hf_token"

vllm_args = {
    "model": model_name,
    "dtype": "half",
    "max-model-len": 2000,
}

hf_secrets = HFSecret(
    secrets_prefix="_FSEC_",
    hf_token_key=hf_token_key
)

vllm_instance = VLLM(
    hf_secret=hf_secrets,
    arg_dict=vllm_args
)

image = fl.ImageSpec(
    name="vllm_serve",
    registry="...",
    packages=["flytekitplugins-inference"],
)


@fl.task(
    pod_template=vllm_instance.pod_template,
    container_image=image,
    secret_requests=[
        fl.Secret(
            key=hf_token_key, mount_requirement=fl.Secret.MountType.ENV_VAR  # must be mounted as an env var
        )
    ],
)
def model_serving() -> str:
    client = OpenAI(
        base_url=f"{vllm_instance.base_url}/v1", api_key="vllm"  # api key required but ignored
    )

    completion = client.chat.completions.create(
        model=model_name,
        messages=[
            {
                "role": "user",
                "content": "Compose a haiku about the power of AI.",
            }
        ],
        temperature=0.5,
        top_p=1,
        max_tokens=1024,
    )
    return completion.choices[0].message.content
```
