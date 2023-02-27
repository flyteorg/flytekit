import os
from tempfile import mkdtemp

from flytekit import FlyteContextManager, LiteralType, StructuredDataset, StructuredDatasetType
from flytekit.core import constants
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.utils import upload_output_file
from flytekit.models import literals


def test_upload_output_file():
    ctx = FlyteContextManager.current_context()
    output_file_dict = {
        constants.OUTPUT_FILE_NAME: literals.LiteralMap(
            {
                "results": TypeEngine.to_literal(
                    ctx,
                    StructuredDataset(uri="dummy_uri"),
                    StructuredDataset,
                    LiteralType(structured_dataset_type=StructuredDatasetType(format="")),
                )
            }
        )
    }
    tmp_dir = mkdtemp(prefix="flyte")
    upload_output_file(output_file_dict, tmp_dir)
    files = os.listdir(tmp_dir)
    assert len(files) == 1
