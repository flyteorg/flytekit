from setuptools import setup

PLUGIN_NAME = "awssagemaker"
INFERENCE_PACKAGE = "awssagemaker_inference"
TRAINING_PACKAGE = "awssagemaker_training"
BATCH_TRANSFORM_PACKAGE = "awssagemaker_batch_transform"
INFERENCE_RECOMMENDER_PACKAGE = "awssagemaker_inference_recommender"
HYPERPARAMETER_TUNING_PACKAGE = "awssagemaker_hyperparameter_tuning"
PROCESSING_PACKAGE = "awssagemaker_processing"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = ["flytekit>1.14.6", "aioboto3>=12.3.0", "xxhash"]

__version__ = "0.0.0+develop"

setup(
    title="AWS SageMaker",
    title_expanded="AWS SageMaker Plugin",
    name=microlib_name,
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="Flytekit AWS SageMaker Plugin",
    namespace_packages=["flytekitplugins"],
    packages=[
        f"flytekitplugins.{INFERENCE_PACKAGE}",
        f"flytekitplugins.{TRAINING_PACKAGE}",
        f"flytekitplugins.{BATCH_TRANSFORM_PACKAGE}",
        f"flytekitplugins.{INFERENCE_RECOMMENDER_PACKAGE}",
        f"flytekitplugins.{HYPERPARAMETER_TUNING_PACKAGE}",
        f"flytekitplugins.{PROCESSING_PACKAGE}",
    ],
    install_requires=plugin_requires,
    license="apache2",
    python_requires=">=3.10",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    entry_points={
        "flytekit.plugins": [
            f"{INFERENCE_PACKAGE}=flytekitplugins.{INFERENCE_PACKAGE}",
            f"{TRAINING_PACKAGE}=flytekitplugins.{TRAINING_PACKAGE}",
            f"{BATCH_TRANSFORM_PACKAGE}=flytekitplugins.{BATCH_TRANSFORM_PACKAGE}",
            f"{INFERENCE_RECOMMENDER_PACKAGE}=flytekitplugins.{INFERENCE_RECOMMENDER_PACKAGE}",
            f"{HYPERPARAMETER_TUNING_PACKAGE}=flytekitplugins.{HYPERPARAMETER_TUNING_PACKAGE}",
            f"{PROCESSING_PACKAGE}=flytekitplugins.{PROCESSING_PACKAGE}",
        ]
    },
)
