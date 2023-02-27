INPUT_FILE_NAME = "inputs.pb"
OUTPUT_FILE_NAME = "outputs.pb"
FUTURES_FILE_NAME = "futures.pb"
ERROR_FILE_NAME = "error.pb"


class SdkTaskType(object):
    PYTHON_TASK = "python-task"
    DYNAMIC_TASK = "dynamic-task"
    CONTAINER_ARRAY_TASK = "container_array"
    SPARK_TASK = "spark"

    # Hive is multi-step operation:
    #    1. a generator task that generates hive-job to be executed by the operator. Generator task is called hive task
    #       for backward compatibility (Note: it is a "batch-task" with a different name)
    #    2. hive-job is the actual set of queries to be executed. This is called hive_job
    BATCH_HIVE_TASK = "batch_hive"
    HIVE_JOB = "hive"
    SIDECAR_TASK = "sidecar"
    SENSOR_TASK = "sensor-task"
    PRESTO_TASK = "presto"
    PYTORCH_TASK = "pytorch"
    TENSORFLOW_TASK = "tensorflow"
    # Raw container task is just a name, it defaults to using the regular container task (like python etc), but sets the data_config in the container
    RAW_CONTAINER_TASK = "raw-container"
    SAGEMAKER_TRAINING_JOB_TASK = "sagemaker_training_job_task"
    SAGEMAKER_CUSTOM_TRAINING_JOB_TASK = "sagemaker_custom_training_job_task"
    SAGEMAKER_HYPERPARAMETER_TUNING_JOB_TASK = "sagemaker_hyperparameter_tuning_job_task"


GLOBAL_INPUT_NODE_ID = ""

START_NODE_ID = "start-node"
END_NODE_ID = "end-node"


class CloudProvider(object):
    AWS = "aws"
    GCP = "gcp"
    LOCAL = "local"
