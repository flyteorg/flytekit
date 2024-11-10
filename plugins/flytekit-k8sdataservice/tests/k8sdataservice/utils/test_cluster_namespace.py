import pytest
from utils.constants import EI, EI_DEV, EI_DEV2, EI_DEV3, GRID1, GRID1_PROD, GRID2, PROD_LTX1
from utils.cluster_namespace import get_execution_namespace


# Test cases for valid cluster inputs
def test_get_execution_namespace_valid_clusters():
    assert get_execution_namespace(EI) == "kk-flyte"
    assert get_execution_namespace(EI_DEV) == "kk-flyte-dev"
    assert get_execution_namespace(EI_DEV2) == "kk-flyte-dev2"
    assert get_execution_namespace(EI_DEV3) == "kk-flyte-dev3"
    assert get_execution_namespace(GRID1) == "kingkong-dev"
    assert get_execution_namespace(GRID1_PROD) == "kk-flyte-prod"
    assert get_execution_namespace(GRID2) == "kingkong-dev"
    assert get_execution_namespace(PROD_LTX1) == "kk-flyte-prod"


def test_get_execution_namespace_invalid_cluster():
    with pytest.raises(Exception) as excinfo:
        get_execution_namespace("INVALID_CLUSTER")
    assert "is not valid or supported yet" in str(excinfo.value)


def test_get_execution_namespace_case_insensitivity():
    assert get_execution_namespace("ei-dev2") == "kk-flyte-dev2"
    assert get_execution_namespace("grid1-prod") == "kk-flyte-prod"
    assert get_execution_namespace("prod-ltx1") == "kk-flyte-prod"
