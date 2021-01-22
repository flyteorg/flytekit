from setuptools import setup

PLUGIN_NAME = "papermill"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = ["flytekit>=0.16.0b0,<1.0.0", "papermill>=1.2.0", "nbconvert>=6.0.7", "ipykernel>=5.0.0"]

setup(
    name=microlib_name,
    version="0.1.0",
    author="flyteorg",
    author_email="admin@flyte.org",
    description="This is the flytekit papermill plugin",
    namespace_packages=["flytekitplugins"],
    packages=[f"flytekitplugins.{PLUGIN_NAME}"],
    install_requires=plugin_requires,
    license="apache2",
    python_requires=">=3.7",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
