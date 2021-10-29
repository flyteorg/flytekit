import flytekit.models.admin.common


def test_identifier():
    obj = flytekit.models.admin.common.NamedEntityIdentifier("proj", "development", "MyWorkflow")
    obj2 = flytekit.models.admin.common.NamedEntityIdentifier.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2


def test_metadata():
    obj = flytekit.models.admin.common.NamedEntityMetadata(
        "i am a description", flytekit.models.admin.common.NamedEntityState.ACTIVE
    )
    obj2 = flytekit.models.admin.common.NamedEntityMetadata.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
