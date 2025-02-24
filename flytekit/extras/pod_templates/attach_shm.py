from flytekit.core.pod_template import PodTemplate


def attach_shm(name: str, size: str) -> PodTemplate:
    from kubernetes.client.models import (
        V1Container,
        V1EmptyDirVolumeSource,
        V1PodSpec,
        V1Volume,
        V1VolumeMount,
    )

    return PodTemplate(
        primary_container_name=name,
        pod_spec=V1PodSpec(
            containers=[V1Container(name=name, volume_mounts=[V1VolumeMount(mount_path="/dev/shm", name="dshm")])],
            volumes=[V1Volume(name="dshm", empty_dir=V1EmptyDirVolumeSource(medium="", size_limit=size))],
        ),
    )
