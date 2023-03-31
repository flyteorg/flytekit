from flytekit.models.security import Secret


def test_secret():
    obj = Secret("grp", "key")
    obj2 = Secret.from_flyte_idl(obj.to_flyte_idl())
    assert obj2.key == "key"
    assert obj2.group_version is None

    obj = Secret("grp", group_version="v1")
    obj2 = Secret.from_flyte_idl(obj.to_flyte_idl())
    assert obj2.key is None
    assert obj2.group_version == "v1"
