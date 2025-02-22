from flytekitplugins.inference import VLLM, HFSecret


def test_vllm_init_valid_params():
    vllm_args = {
        "model": "google/gemma-2b-it",
        "dtype": "half",
        "max-model-len": 2000,
    }

    hf_secrets = HFSecret(
        secrets_prefix="_UNION_",
        hf_token_key="vllm_hf_token"
    )

    vllm_instance = VLLM(
        hf_secret=hf_secrets,
        arg_dict=vllm_args,
        image='vllm/vllm-openai:my-tag',
        cpu='10',
        gpu='2',
        mem='50Gi',
        port=8080,
    )

    assert len(vllm_instance.pod_template.pod_spec.init_containers) == 1
    assert (
        vllm_instance.pod_template.pod_spec.init_containers[0].image
        == 'vllm/vllm-openai:my-tag'
    )
    assert (
        vllm_instance.pod_template.pod_spec.init_containers[0].resources.requests[
            "memory"
        ]
        == "50Gi"
    )
    assert (
        vllm_instance.pod_template.pod_spec.init_containers[0].ports[0].container_port
        == 8080
    )
    assert vllm_instance.pod_template.pod_spec.init_containers[0].args == ['--model', 'google/gemma-2b-it', '--dtype', 'half', '--max-model-len', '2000']
    assert vllm_instance.pod_template.pod_spec.init_containers[0].env[0].name == 'HUGGING_FACE_HUB_TOKEN'
    assert vllm_instance.pod_template.pod_spec.init_containers[0].env[0].value == '$(_UNION_VLLM_HF_TOKEN)'



def test_vllm_default_params():
    vllm_instance = VLLM(hf_secret=HFSecret(secrets_prefix="_FSEC_", hf_token_key="test_token"))

    assert vllm_instance.base_url == "http://localhost:8000"
    assert vllm_instance._image == 'vllm/vllm-openai'
    assert vllm_instance._port == 8000
    assert vllm_instance._cpu == 2
    assert vllm_instance._gpu == 1
    assert vllm_instance._health_endpoint == "/health"
    assert vllm_instance._mem == "10Gi"
    assert vllm_instance._arg_dict == None
    assert vllm_instance._hf_secret.secrets_prefix == '_FSEC_'
    assert vllm_instance._hf_secret.hf_token_key == 'test_token'
    assert vllm_instance._hf_secret.hf_token_group == None
