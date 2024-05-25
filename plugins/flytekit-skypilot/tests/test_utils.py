import unittest
from flytekitplugins.skypilot import utils

def test_execute_cmd_to_path():
    random_dir = "/tmp/abc"
    args = ["pyflyte-fast-execute",
        "--additional-distribution",
        "{{ .remote_package_path }}",
        "--dest-dir",
        "{{ .dest_dir }}",
        "--",
        "pyflyte-execute",
        "--inputs",
        "s3://becket/inputs.pb",
        "--output-prefix",
        "s3://becket",
        "--raw-output-data-prefix",
        f"{random_dir}",
        "--checkpoint-path",
        "s3://becket/checkpoint_output",
        "--prev-checkpoint",
        "s3://becket/prev_checkpoint",
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "test_async_agent",
        "task-name",
        "say_hello0",
    ]
    
    new_args = utils.execute_cmd_to_path(args)
    assert new_args["raw_output_data_prefix"] == random_dir