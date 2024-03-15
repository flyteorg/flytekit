from setuptools import setup

PLUGIN_NAME = "awssagemaker"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

# s3fs 2023.9.2 requires aiobotocore~=2.5.4
plugin_requires = ["flytekit>1.10.7", "flyteidl>=1.11.0b0", "aioboto3==11.1.1"]

__version__ = "0.0.0+develop"

setup(
    name=microlib_name,
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="Flytekit AWS SageMaker Plugin",
    namespace_packages=["flytekitplugins"],
    packages=["flytekitplugins.awssagemaker_inference"],
    install_requires=plugin_requires,
    license="apache2",
    python_requires=">=3.8",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    entry_points={"flytekit.plugins": ["awssagemaker_inference=flytekitplugins.awssagemaker_inference"]},
)
