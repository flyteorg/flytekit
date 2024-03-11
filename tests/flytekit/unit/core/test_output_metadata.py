import base64
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import mock
import py
import pytest

import flytekit.configuration.plugin
from flytekit.configuration import (
    SERIALIZED_CONTEXT_ENV_VAR,
    FastSerializationSettings,
    Image,
    ImageConfig,
    SecretsConfig,
    SerializationSettings,
)
from flytekit.core import mock_stats
from flytekit.core.context_manager import ExecutionParameters, FlyteContext, FlyteContextManager, SecretsManager
from flytekit.models.core import identifier as id_models


