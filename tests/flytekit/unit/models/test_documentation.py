from flytekit.models.documentation import Documentation, LongDescription, SourceCode


def test_long_description():
    value = "long"
    icon_link = "http://icon"
    obj = LongDescription(value=value, icon_link=icon_link)
    assert LongDescription.from_flyte_idl(obj.to_flyte_idl()) == obj
    assert obj.value == value
    assert obj.icon_link == icon_link
    assert obj.format == LongDescription.DescriptionFormat.RST


def test_source_code():
    link = "https://github.com/flyteorg/flytekit"
    obj = SourceCode(link=link)
    assert SourceCode.from_flyte_idl(obj.to_flyte_idl()) == obj
    assert obj.link == link


def test_documentation():
    short_description = "short"
    long_description = LongDescription(value="long", icon_link="http://icon")
    source_code = SourceCode(link="https://github.com/flyteorg/flytekit")
    obj = Documentation(short_description=short_description, long_description=long_description, source_code=source_code)
    assert Documentation.from_flyte_idl(obj.to_flyte_idl()) == obj
    assert obj.short_description == short_description
    assert obj.long_description == long_description
    assert obj.source_code == source_code
