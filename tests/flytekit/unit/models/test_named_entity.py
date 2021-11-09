from flytekit.models.admin import common as admin_models


def test_identifier():
    obj = admin_models.NamedEntityIdentifier("proj", "development", "MyWorkflow")
    obj2 = admin_models.NamedEntityIdentifier.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2


def test_metadata():
    obj = admin_models.NamedEntityMetadata("i am a description", admin_models.NamedEntityState.ACTIVE)
    obj2 = admin_models.NamedEntityMetadata.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
