from setuptools import setup

PLUGIN_NAME = "onnxpytorch"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = ["flytekit>=1.1.0b0,<2.0.0", "torch>=1.11.0"]

__version__ = "0.0.0+develop"

setup(
    name=f"flytekitplugins-{PLUGIN_NAME}",
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="ONNX PyTorch Plugin for Flytekit",
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
