from flytekit.core.inference import NIM
import pytest

secrets = {
    "ngc_secret_group": "ngc-credentials",
    "ngc_secret_key": "api_key",
    "ngc_image_secret": "nvcrio-cred",
}


def test_nim_init_raises_value_error():
    with pytest.raises(ValueError):
        NIM(
            ngc_image_secret=secrets["ngc_image_secret"],
            ngc_secret_key=secrets["ngc_secret_key"],
        )

    with pytest.raises(ValueError):
        NIM(
            ngc_secret_group=secrets["ngc_secret_group"],
            ngc_secret_key=secrets["ngc_secret_key"],
        )


def test_nim_secrets():
    nim_instance = NIM(
        image="nvcr.io/nim/meta/llama3-8b-instruct:1.0.0",
        node_selector={"k8s.amazonaws.com/accelerator": "nvidia-tesla-l4"},
        **secrets,
    )

    assert (
        nim_instance.pod_template.pod_spec.image_pull_secrets[0].name == "nvcrio-cred"
    )
    secret_obj = (
        nim_instance.pod_template.pod_spec.init_containers[0]
        .env[0]
        .value_from.secret_key_ref
    )
    assert secret_obj.name == "ngc-credentials"
    assert secret_obj.key == "api_key"


def test_nim_init_valid_params():
    nim_instance = NIM(
        mem="30Gi",
        port=8002,
        image="nvcr.io/nim/meta/llama3-8b-instruct:1.0.0",
        node_selector={"k8s.amazonaws.com/accelerator": "nvidia-tesla-l4"},
        **secrets,
    )

    assert nim_instance.pod_template.pod_spec.node_selector == {
        "k8s.amazonaws.com/accelerator": "nvidia-tesla-l4"
    }
    assert (
        nim_instance.pod_template.pod_spec.init_containers[0].image
        == "nvcr.io/nim/meta/llama3-8b-instruct:1.0.0"
    )
    assert (
        nim_instance.pod_template.pod_spec.init_containers[0].resources.requests[
            "memory"
        ]
        == "30Gi"
    )
    assert (
        nim_instance.pod_template.pod_spec.init_containers[0].ports[0].container_port
        == 8002
    )


def test_nim_default_params():
    nim_instance = NIM(**secrets)

    assert nim_instance.base_url == "http://localhost:8000"
    assert nim_instance._cpu == 1
    assert nim_instance._gpu == 1
    assert nim_instance._health_endpoint == "v1/health/ready"
    assert nim_instance._mem == "20Gi"
    assert nim_instance._shm_size == "16Gi"
