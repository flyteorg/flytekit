######################
Data Persistence Layer
######################

Flytekit provides a data persistence layer, which is used for recording metadata that is shared with the Flyte backend. This persistence layer is available for various types to store raw user data and is designed to be cross-cloud compatible.
Moreover, it is designed to be extensible and users can bring their own data persistence plugins by following the persistence interface.

.. note::
   This will become extensive for a variety of use-cases, but the core set of APIs have been battle tested.

.. automodule:: flytekit.core.data_persistence
    :no-members:
    :no-inherited-members:
    :no-special-members:

.. automodule:: flytekit.extras.persistence
    :no-members:
    :no-inherited-members:
    :no-special-members:

The ``fsspec`` Data Plugin
--------------------------

Flytekit ships with a default storage driver that uses aws-cli on AWS and gsutil on GCP. By default, Flyte uploads the task outputs to S3 or GCS using these storage drivers.

Why ``fsspec``?
^^^^^^^^^^^^^^^

You can use the fsspec plugin implementation to utilize all its available plugins with flytekit. The `fsspec <https://github.com/flyteorg/flytekit/tree/aad4e0e67fbc1614b4aa3e49c49cd777738cd3d7/plugins/flytekit-data-fsspec>`_ plugin provides an implementation of the data persistence layer in Flytekit. For example: HDFS, FTP are supported in fsspec, so you can use them with flytekit too.
The data persistence layer helps store logs of metadata and raw user data.
As a consequence of the implementation, an S3 driver can be installed using ``pip install s3fs``.

`Here <https://github.com/fsspec/filesystem_spec/blob/ffe57d6eabe517b4c39c27487fc45b804d314b58/fsspec/registry.py#L87-L205>`_ is a code snippet that shows protocols mapped to the class it implements.

Once you install the plugin, it overrides all default implementations of the `DataPersistencePlugins <https://github.com/flyteorg/flytekit/blob/5907e766a01058181697de2babd779588e5d48b0/flytekit/core/data_persistence.py#L107-L116>`_ and provides the ones supported by fsspec.

.. note::
   This plugin installs fsspec core only. To install all the fsspec plugins, see `here <https://filesystem-spec.readthedocs.io/en/latest/>`_.
