from flytekitplugins.awsbatch import AWSBatchConfig
from collections import OrderedDict

from flytekit import PythonFunctionTask, task, map_task, TaskMetadata
from flytekit.extend import Image, ImageConfig, SerializationSettings
from flytekit.tools.translator import get_serializable

config = AWSBatchConfig(
    parameters={"codec": "mp4"},
    platformCapabilities="EC2",
    propagateTags=True,
    tags={"hello": "world"},
)


def test_aws_batch_task():
    @task(task_config=config)
    def t1(a: int) -> str:
        inc = a + 2
        return str(inc)

    assert t1.task_config is not None
    assert t1.task_config == config
    assert t1.task_type == "aws-batch"
    assert isinstance(t1, PythonFunctionTask)

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    assert t1.get_custom(settings) == config.to_dict()
    assert t1.get_command(settings) == [
        "pyflyte-execute",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}/0",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "tests.test_aws_batch",
        "task-name",
        "t1",
    ]


def test_aws_batch_mapped():
    @task(task_config=config)
    def t1(a: int) -> str:
        inc = a + 2
        return str(inc)

    maptask = map_task(t1, metadata=TaskMetadata(retries=1))

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    task_spec = get_serializable(OrderedDict(), settings, maptask)
    print(task_spec)
    """
    template {
      id {
        resource_type: TASK
        project: "project"
        domain: "domain"
        name: "tests.test_aws_batch.mapper_t1_0"
        version: "version"
      }
      type: "container_array"
      metadata {
        runtime {
          type: FLYTE_SDK
          version: "0.0.0+develop"
          flavor: "python"
        }
        retries {
          retries: 1
        }
      }
      interface {
        inputs {
          variables {
            key: "a"
            value {
              type {
                collection_type {
                  simple: INTEGER
                }
              }
              description: "a"
            }
          }
        }
        outputs {
          variables {
            key: "o0"
            value {
              type {
                collection_type {
                  simple: STRING
                }
              }
              description: "o0"
            }
          }
        }
      }
      custom {
        fields {
          key: "minSuccessRatio"
          value {
            number_value: 1.0
          }
        }
      }
      container {
        image: "test:tag"
        args: "pyflyte-execute"
        args: "--inputs"
        args: "{{.input}}"
        args: "--output-prefix"
        args: "{{.outputPrefix}}/0"
        args: "--raw-output-data-prefix"
        args: "{{.rawOutputDataPrefix}}"
        args: "--resolver"
        args: "flytekit.core.python_auto_container.default_task_resolver"
        args: "--"
        args: "task-module"
        args: "tests.test_aws_batch"
        args: "task-name"
        args: "t1"
        resources {
        }
        env {
          key: "FOO"
          value: "baz"
        }
      }
      task_type_version: 1
      config {
        key: "platformCapabilities"
        value: "EC2"
      }
    }
    """
