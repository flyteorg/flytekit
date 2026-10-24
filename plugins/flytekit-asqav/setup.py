from setuptools import setup

PLUGIN_NAME = "asqav"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = ["flytekit>=1.12.0", "asqav>=0.1.0"]

__version__ = "0.0.0+develop"

setup(
    title="Asqav",
    title_expanded="Flytekit Asqav Audit Trail Plugin",
    name=microlib_name,
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="Cryptographic audit trails for Flyte tasks using Asqav",
    namespace_packages=["flytekitplugins"],
    packages=[f"flytekitplugins.{PLUGIN_NAME}"],
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
)
