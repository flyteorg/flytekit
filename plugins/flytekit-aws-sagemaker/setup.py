from setuptools import setup

PLUGIN_NAME = "awssagemaker"
INFERENCE_PACKAGE = "awssagemaker_inference"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = ["flytekit>=1.11.0", "aioboto3>=12.3.0", "xxhash"]

__version__ = "0.0.0+develop"

setup(
    name=microlib_name,
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="Flytekit AWS SageMaker Plugin",
    namespace_packages=["flytekitplugins"],
    packages=[f"flytekitplugins.{INFERENCE_PACKAGE}"],
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
    entry_points={"flytekit.plugins": [f"{INFERENCE_PACKAGE}=flytekitplugins.{INFERENCE_PACKAGE}"]},
)
