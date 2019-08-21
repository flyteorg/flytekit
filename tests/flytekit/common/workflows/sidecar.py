from __future__ import absolute_import
from __future__ import print_function

import os
import time

from flytekit.sdk.tasks import sidecar_task
from flytekit.sdk.workflow import workflow_class, Input
from flytekit.sdk.types import Types
from k8s.io.api.core.v1 import generated_pb2


def generate_pod_spec_for_task():
    pod_spec = generated_pb2.PodSpec()
    secondary_container = generated_pb2.Container(
        name="secondary",
        image="alpine",
    )
    secondary_container.command.extend(["/bin/sh"])
    secondary_container.args.extend(["-c", "echo hi sidecar world > /data/message.txt"])
    shared_volume_mount = generated_pb2.VolumeMount(
        name="shared-data",
        mountPath="/data",
    )
    secondary_container.volumeMounts.extend([shared_volume_mount])

    primary_container = generated_pb2.Container(name="primary")
    primary_container.volumeMounts.extend([shared_volume_mount])

    pod_spec.volumes.extend([generated_pb2.Volume(
        name="shared-data",
        volumeSource=generated_pb2.VolumeSource(
            emptyDir=generated_pb2.EmptyDirVolumeSource(
                medium="Memory",
            )
        )
    )])
    pod_spec.containers.extend([primary_container, secondary_container])
    return pod_spec


@sidecar_task(
    pod_spec=generate_pod_spec_for_task(),
    primary_container_name="primary",
)
def a_sidecar_task(wfparams):
    while not os.path.isfile('/data/message.txt'):
        time.sleep(5)


@workflow_class
class SimpleSidecarWorkflow(object):
    input_1 = Input(Types.String)
    my_sidecar_task = a_sidecar_task()
