from flytekitplugins.inference import Ollama, Model


def test_ollama_init_valid_params():
    ollama_instance = Ollama(
        mem="30Gi",
        port=11435,
        model=Model(name="mistral-nemo"),
    )

    assert len(ollama_instance.pod_template.pod_spec.init_containers) == 2
    assert (
        ollama_instance.pod_template.pod_spec.init_containers[0].image
        == "ollama/ollama"
    )
    assert (
        ollama_instance.pod_template.pod_spec.init_containers[0].resources.requests[
            "memory"
        ]
        == "30Gi"
    )
    assert (
        ollama_instance.pod_template.pod_spec.init_containers[0].ports[0].container_port
        == 11435
    )
    assert (
        "mistral-nemo"
        in ollama_instance.pod_template.pod_spec.init_containers[1].args[0]
    )
    assert (
        "ollama.pull"
        in ollama_instance.pod_template.pod_spec.init_containers[1].args[0]
    )


def test_ollama_default_params():
    ollama_instance = Ollama(model=Model(name="phi"))

    assert ollama_instance.base_url == "http://localhost:11434"
    assert ollama_instance._cpu == 1
    assert ollama_instance._gpu == 1
    assert ollama_instance._health_endpoint == None
    assert ollama_instance._mem == "15Gi"
    assert ollama_instance._model_name == "phi"
    assert ollama_instance._model_cpu == 1
    assert ollama_instance._model_mem == "500Mi"


def test_ollama_modelfile():
    ollama_instance = Ollama(
        model=Model(
            name="llama3-mario",
            modelfile="FROM llama3\nPARAMETER temperature 1\nPARAMETER num_ctx 4096\nSYSTEM You are Mario from super mario bros, acting as an assistant.",
        )
    )

    assert len(ollama_instance.pod_template.pod_spec.init_containers) == 2
    assert (
        "ollama.create"
        in ollama_instance.pod_template.pod_spec.init_containers[1].args[0]
    )
    assert (
        "format(**inputs)"
        not in ollama_instance.pod_template.pod_spec.init_containers[1].args[0]
    )


def test_ollama_modelfile_with_inputs():
    ollama_instance = Ollama(
        model=Model(
            name="tinyllama-finetuned",
            modelfile='''FROM tinyllama:latest
ADAPTER {inputs.ggml}
TEMPLATE """Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.

{{ if .System }}### Instruction:
{{ .System }}{{ end }}

{{ if .Prompt }}### Input:
{{ .Prompt }}{{ end }}

### Response:
"""
SYSTEM "You're a kitty. Answer using kitty sounds."
PARAMETER stop "### Response:"
PARAMETER stop "### Instruction:"
PARAMETER stop "### Input:"
PARAMETER stop "Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request."
PARAMETER num_predict 200
''',
        )
    )

    assert len(ollama_instance.pod_template.pod_spec.init_containers) == 3
    assert (
        "model-server" in ollama_instance.pod_template.pod_spec.init_containers[0].name
    )
    assert (
        "input-downloader"
        in ollama_instance.pod_template.pod_spec.init_containers[1].name
    )
    assert (
        "ollama.create"
        in ollama_instance.pod_template.pod_spec.init_containers[2].args[0]
    )
    assert (
        "format(**inputs)"
        in ollama_instance.pod_template.pod_spec.init_containers[2].args[0]
    )
