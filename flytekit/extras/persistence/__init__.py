"""
=======================
DataPersistence Extras
=======================

.. currentmodule:: flytekit.extras.persistence

This module provides some default implementations of :py:class:`flytekit.DataPersistence`. These implementations
use command-line clients to download and upload data. The actual binaries need to be installed for these extras to work.
The binaries are not bundled with flytekit to keep it lightweight.

Persistence Extras
===================

.. autosummary::
   :template: custom.rst
   :toctree: generated/

    GCSPersistence
    HttpPersistence
    S3Persistence
"""

from flytekit.extras.persistence.gcs_gsutil import GCSPersistence
from flytekit.extras.persistence.http import HttpPersistence
from flytekit.extras.persistence.s3_awscli import S3Persistence
