from setuptools import setup

PLUGIN_NAME = "appliedflyteinteractive"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

# dev is required because flytekit brought in through
# https://github.com/flyteorg/flytekit/compare/master...ongkong:flytekit:ongkong/fix-external-command-auth
plugin_requires = ["flytekit==0.1.dev1583+gd1616ad", "jupyter", "kubernetes"]
# Switch to run pytest.
#  plugin_requires = ["flytekit>=1.1.0b0,<2.0.0", "jupyter", "kubernetes"]

__version__ = "0.0.0+develop"

setup(
    name=microlib_name,
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="This package holds the applied flyteinteractive plugins for flytekit",
    namespace_packages=["flytekitplugins"],
    packages=[
        f"flytekitplugins.{PLUGIN_NAME}",
        f"flytekitplugins.{PLUGIN_NAME}.jupyter_lib",
    ],
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
    entry_points={"flytekit.plugins": [f"{PLUGIN_NAME}=flytekitplugins.{PLUGIN_NAME}"]},
)
