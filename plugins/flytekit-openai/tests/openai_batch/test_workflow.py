from flytekitplugins.openai import create_batch


def test_openai_batch_wf():
    openai_batch_wf = create_batch(
        name="gpt-3.5-turbo",
        openai_organization="testorg",
    )

    assert len(openai_batch_wf.interface.inputs) == 1
    assert len(openai_batch_wf.interface.outputs) == 1
    assert len(openai_batch_wf.interface.nodes) == 3
