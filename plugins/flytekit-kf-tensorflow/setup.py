from setuptools import setup

PLUGIN_NAME = "kftensorflow"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = ["flyteidl>=1.10.0", "flytekit>=1.6.1"]

__version__ = "0.0.0+develop"

setup(
    title="Kubeflow TensorFlow",
    title_expanded="Flytekit Kubeflow TensorFlow Plugin",
    name=microlib_name,
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="K8s based Tensorflow plugin for flytekit",
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
