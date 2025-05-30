from setuptools import setup

PLUGIN_NAME = "spark"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

# TODO: Add support spark 4.0.0, https://github.com/flyteorg/flyte/issues/6478
plugin_requires = ["flytekit>=1.15.1", "pyspark>=3.4.0,<4.0.0", "aiohttp", "flyteidl>=1.11.0b1", "pandas"]

__version__ = "0.0.0+develop"

setup(
    title="Spark",
    title_expanded="Flytekit Spark Plugin",
    name=microlib_name,
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="Spark 3 plugin for flytekit",
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
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    scripts=["scripts/flytekit_install_spark3.sh"],
    entry_points={"flytekit.plugins": [f"{PLUGIN_NAME}=flytekitplugins.{PLUGIN_NAME}"]},
)
