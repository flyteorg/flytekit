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
