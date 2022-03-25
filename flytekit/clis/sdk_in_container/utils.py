from typing import List

from google.protobuf.pyext.cpp_message import GeneratedProtocolMessageType


def produce_fast_register_task_closure(full_remote_path, destination_dir):
    def fast_register_task(entity: GeneratedProtocolMessageType) -> GeneratedProtocolMessageType:
        """
        Updates task definitions during fast-registration in order to use the compatible pyflyte fast execute command at
        task execution.
        """
        # entity is of type flyteidl.admin.task_pb2.TaskSpec

        def _substitute_fast_register_task_args(args: List[str], full_remote_path: str, dest_dir: str) -> List[str]:
            complete_args = []
            for arg in args:
                if arg == "{{ .remote_package_path }}":
                    arg = full_remote_path
                elif arg == "{{ .dest_dir }}":
                    arg = dest_dir if dest_dir else "."
                complete_args.append(arg)
            return complete_args

        if entity.template.HasField("container") and len(entity.template.container.args) > 0:
            complete_args = _substitute_fast_register_task_args(
                entity.template.container.args, full_remote_path, destination_dir
            )
            # Because we're dealing with a proto list, we have to delete the existing args before we can extend the list
            # with the substituted ones.
            del entity.template.container.args[:]
            entity.template.container.args.extend(complete_args)

        if entity.template.HasField("k8s_pod"):
            pod_spec_struct = entity.template.k8s_pod.pod_spec
            if "containers" in pod_spec_struct:
                for idx in range(len(pod_spec_struct["containers"])):
                    if "args" in pod_spec_struct["containers"][idx]:
                        # We can directly overwrite the args in the pod spec struct definition.
                        pod_spec_struct["containers"][idx]["args"] = _substitute_fast_register_task_args(
                            pod_spec_struct["containers"][idx]["args"], full_remote_path, destination_dir
                        )
        return entity

    return fast_register_task
