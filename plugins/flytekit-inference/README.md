# Inference Plugins

To install the plugin, run the following command:

```bash
pip install flytekitplugins-inference
```

## NIM

The NIM plugin allows you to serve optimized model containers that can include
NVIDIA CUDA software, NVIDIA Triton Inference SErver and NVIDIA TensorRT-LLM software.

```python
from flytekit import ImageSpec, Resources, task
from flytekitplugins.inference import NIM
from openai import OpenAI

image = ImageSpec(
    name="nim",
    registry="...",
    packages=["flytekitplugins-inference"],
)

nim_instance = NIM(
    image="nvcr.io/nim/meta/llama3-8b-instruct:1.0.0",
    node_selector={"k8s.amazonaws.com/accelerator": "nvidia-tesla-l4"},
    ngc_secret_group="ngc-credentials",
    ngc_secret_key="api_key",
    ngc_image_secret="nvcrio-cred",
)


@task(
    container_image=image,
    requests=Resources(cpu="1", gpu="0", mem="1Gi"),
    pod_template=nim_instance.pod_template,
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
