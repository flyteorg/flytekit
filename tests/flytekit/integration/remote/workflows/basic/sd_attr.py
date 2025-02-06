from dataclasses import dataclass

import pandas as pd
from flytekit import task, workflow
from flytekit.types.structured import StructuredDataset


@dataclass
class DC:
    sd: StructuredDataset


@task
def create_dc(uri: str, file_format: str) -> DC:
    """Create a dataclass with a StructuredDataset attribute.

    Args:
        uri: File URI.
        file_format: File format, e.g., parquet, csv.

    Returns:
        dc: A dataclass with a StructuredDataset attribute.
    """
    dc = DC(sd=StructuredDataset(uri=uri, file_format=file_format))

    return dc


@task
def check_file_format(sd: StructuredDataset, true_file_format: str) -> StructuredDataset:
    """Check StructuredDataset file_format attribute.

    StruturedDataset file_format should align with what users specify.

    Args:
        sd: Python native StructuredDataset.
        true_file_format: User-specified file_format.
    """
    assert sd.file_format == true_file_format, (
        f"StructuredDataset file_format should align with the user-specified file_format: {true_file_format}."
    )
    assert sd._literal_sd.metadata.structured_dataset_type.format == true_file_format, (
        f"StructuredDatasetType format should align with the user-specified file_format: {true_file_format}."
    )
    print(f">>> SD <<<\n{sd}")
    print(f">>> Literal SD <<<\n{sd._literal_sd}")
    print(f">>> SDT <<<\n{sd._literal_sd.metadata.structured_dataset_type}")
    print(f">>> DF <<<\n{sd.open(pd.DataFrame).all()}")

    return sd


@workflow
def wf(dc: DC, file_format: str) -> StructuredDataset:
    # Fail to use dc.sd.file_format as the input
    sd = check_file_format(sd=dc.sd, true_file_format=file_format)

    return sd


if __name__ == "__main__":
    # Define inputs
    uri = "tests/flytekit/integration/remote/workflows/basic/data/df.parquet"
    file_format = "parquet"

    dc = create_dc(uri=uri, file_format=file_format)
    sd = wf(dc=dc, file_format=file_format)
    print(sd.file_format)
