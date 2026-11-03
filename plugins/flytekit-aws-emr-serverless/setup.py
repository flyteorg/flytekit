from setuptools import setup

PLUGIN_NAME = "awsemrserverless"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = [
    "flytekit>=1.14.6",
    "aioboto3>=12.3.0",
    "boto3>=1.28.0",
]

__version__ = "0.0.0+develop"

setup(
    title="AWS EMR Serverless",
    title_expanded="Flytekit AWS EMR Serverless Plugin",
    name=microlib_name,
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="Flytekit AWS EMR Serverless Plugin: run Spark and Hive jobs from Flyte tasks",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    namespace_packages=["flytekitplugins"],
    packages=[f"flytekitplugins.{PLUGIN_NAME}"],
    install_requires=plugin_requires,
    license="apache2",
    python_requires=">=3.9",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.9",
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
            f"{PLUGIN_NAME}=flytekitplugins.{PLUGIN_NAME}",
        ],
    },
)
