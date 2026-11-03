from setuptools import setup

PLUGIN_NAME = "onnxscikitlearn"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

# flytekit>=1.16.29: first release whose lazy_module does not execute skl2onnx.common.data_types twice, which
# skl2onnx>=1.19 needs.
plugin_requires = ["flytekit>=1.16.29", "skl2onnx>=1.10.3", "networkx<3.2; python_version<'3.9'"]

__version__ = "0.0.0+develop"

setup(
    title="ONNX ScikitLearn",
    title_expanded="Flytekit ONNX ScikitLearn Plugin",
    name=f"flytekitplugins-{PLUGIN_NAME}",
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="ONNX ScikitLearn Plugin for Flytekit",
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
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
