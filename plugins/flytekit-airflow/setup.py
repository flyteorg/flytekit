from setuptools import setup

PLUGIN_NAME = "airflow"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = [
    "apache-airflow<3.0.0",
    "apache-airflow-providers-google<12.0.0",
    "flytekit>1.10.7",
    "flyteidl>1.10.7",
    "sqlalchemy-spanner<1.12.0",
    "google-re2>=1.0,!=1.1.20251105",  # Avoid version with Bazel build issues
]

__version__ = "0.0.0+develop"

setup(
    title="Apache Airflow",
    title_expanded="Flytekit Airflow Plugin",
    name=microlib_name,
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="This package holds the Airflow plugins for flytekit",
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
    entry_points={"flytekit.plugins": [f"{PLUGIN_NAME}=flytekitplugins.{PLUGIN_NAME}"]},
)
