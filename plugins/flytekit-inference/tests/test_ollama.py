from flytekitplugins.inference import Ollama


def test_ollama_init_valid_params():
    ollama_instance = Ollama(
        server_mem="30Gi",
        port=11435,
        model="mistral-nemo",
    )

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
        in ollama_instance.pod_template.pod_spec.init_containers[1].command[2]
    )


def test_ollama_default_params():
    ollama_instance = Ollama()

    assert ollama_instance.base_url == "http://localhost:11434"
    assert ollama_instance._cpu == 1
    assert ollama_instance._gpu == 1
    assert ollama_instance._health_endpoint == None
    assert ollama_instance._mem == "15Gi"
    assert ollama_instance._model == "llama3:8b-instruct-fp16"
    assert ollama_instance._model_cpu == 1
    assert ollama_instance._model_gpu == 0
    assert ollama_instance._model_mem == "10Gi"
