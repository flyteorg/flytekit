from flytekit.models import node_execution as node_execution_models


def test_metadata():
    md = node_execution_models.NodeExecutionMetaData(retry_group="0", is_parent_node=True, spec_node_id="n0")
    md2 = node_execution_models.NodeExecutionMetaData.from_flyte_idl(md.to_flyte_idl())
    assert md == md2
