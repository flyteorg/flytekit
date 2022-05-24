from setuptools import setup

PLUGIN_NAME = "whylogs"

# TODO decide if the plugin is regular or `data` for regular plugins
microlib_name = f"flytekitplugins-{PLUGIN_NAME}"
# microlib_name = f"flytekitplugins-data-{PLUGIN_NAME}"

# TODO add additional requirements. This has to be updated when whylogs v1 is published and probably blocks a push now.
plugin_requires = ["whylogs @ file:///Users/ytong/whylogs-1.0.0rc0-py3-none-any.whl"]

__version__ = "0.0.0+develop"

setup(
    name=microlib_name,
    version=__version__,
    author="flyteorg",  # TODO should this be whylabs or flyte?
    author_email="admin@flyte.org",
    # TODO Probably make this better after checking with the team.
    description="Enable the use of whylogs profiles to be used in flyte tasks to get aggregate statistics about data.",
    url="https://github.com/flyteorg/flytekit/tree/master/plugins/flytekit-whylogs",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    namespace_packages=["flytekitplugins"],
    packages=[f"flytekitplugins.{PLUGIN_NAME}"],
    install_requires=plugin_requires,
    license="apache2",
    python_requires=">=3.7",
    # TODO what are we?
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    # TODO Something about this isn't working as it is.
    entry_points={"flytekit.plugins": [f"{PLUGIN_NAME}=flytekitplugins.{PLUGIN_NAME}"]},
)
