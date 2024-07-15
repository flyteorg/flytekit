import pytest
from dataclasses_json import DataClassJsonMixin
from mashumaro.mixins.json import DataClassJSONMixin
import os
import tempfile
from dataclasses import dataclass
from typing import Annotated, List, Dict, Optional

from flytekit.core.type_engine import TypeEngine
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import DataclassTransformer
from flytekit import task, workflow
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekit.types.structured import StructuredDataset

@pytest.fixture
def local_dummy_txt_file():
    fd, path = tempfile.mkstemp(suffix=".txt")
    try:
        with os.fdopen(fd, "w") as tmp:
            tmp.write("Hello World")
        yield path
    finally:
        os.remove(path)

@pytest.fixture
def local_dummy_directory():
    temp_dir = tempfile.TemporaryDirectory()
    try:
        with open(os.path.join(temp_dir.name, "file"), "w") as tmp:
            tmp.write("Hello world")
        yield temp_dir.name
    finally:
        temp_dir.cleanup()

def test_dataclass():
    @dataclass
    class AppParams(DataClassJsonMixin):
        snapshotDate: str
        region: str
        preprocess: bool
        listKeys: List[str]

    @task
    def t1() -> AppParams:
        ap = AppParams(snapshotDate="4/5/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"])
        return ap

    @workflow
    def wf() -> AppParams:
        return t1()

    res = wf()
    assert res.region == "us-west-3"


def test_dataclass_assert_works_for_annotated():
    @dataclass
    class MyDC(DataClassJSONMixin):
        my_str: str

    d = Annotated[MyDC, "tag"]
    DataclassTransformer().assert_type(d, MyDC(my_str="hi"))

def test_pure_dataclasses_with_python_types():
    @dataclass
    class DC:
        string: Optional[str] = None

    @dataclass
    class DCWithOptional:
        string: Optional[str] = None
        dc: Optional[DC] = None
        list_dc: Optional[List[DC]] = None
        list_list_dc: Optional[List[List[DC]]] = None
        dict_dc: Optional[Dict[str, DC]] = None
        dict_dict_dc: Optional[Dict[str, Dict[str, DC]]] = None
        dict_list_dc: Optional[Dict[str, List[DC]]] = None
        list_dict_dc: Optional[List[Dict[str, DC]]] = None

    @task
    def t1() -> DCWithOptional:
        return DCWithOptional(string="a", dc=DC(string="b"),
                              list_dc=[DC(string="c"), DC(string="d")],
                              list_list_dc=[[DC(string="e"), DC(string="f")]],
                              list_dict_dc=[{"g": DC(string="h"), "i": DC(string="j")},
                                            {"k": DC(string="l"), "m": DC(string="n")}],
                              dict_dc={"o": DC(string="p"), "q": DC(string="r")},
                              dict_dict_dc={"s": {"t": DC(string="u"), "v": DC(string="w")}},
                              dict_list_dc={"x": [DC(string="y"), DC(string="z")],
                                            "aa": [DC(string="bb"), DC(string="cc")]},)

    @task
    def t2() -> DCWithOptional:
        return DCWithOptional()

    output = DCWithOptional(string="a", dc=DC(string="b"),
                            list_dc=[DC(string="c"), DC(string="d")],
                            list_list_dc=[[DC(string="e"), DC(string="f")]],
                            list_dict_dc=[{"g": DC(string="h"), "i": DC(string="j")},
                                          {"k": DC(string="l"), "m": DC(string="n")}],
                            dict_dc={"o": DC(string="p"), "q": DC(string="r")},
                            dict_dict_dc={"s": {"t": DC(string="u"), "v": DC(string="w")}},
                            dict_list_dc={"x": [DC(string="y"), DC(string="z")],
                                          "aa": [DC(string="bb"), DC(string="cc")]}, )

    dc1 = t1()
    dc2 = t2()

    assert dc1 == output
    assert dc2.string is None
    assert dc2.dc is None

    DataclassTransformer().assert_type(DCWithOptional, dc1)
    DataclassTransformer().assert_type(DCWithOptional, dc2)


def test_pure_dataclasses_with_python_types_get_literal_type_and_to_python_value():
    @dataclass
    class DC:
        string: Optional[str] = None

    @dataclass
    class DCWithOptional:
        string: Optional[str] = None
        dc: Optional[DC] = None
        list_dc: Optional[List[DC]] = None
        list_list_dc: Optional[List[List[DC]]] = None
        dict_dc: Optional[Dict[str, DC]] = None
        dict_dict_dc: Optional[Dict[str, Dict[str, DC]]] = None
        dict_list_dc: Optional[Dict[str, List[DC]]] = None
        list_dict_dc: Optional[List[Dict[str, DC]]] = None

    ctx = FlyteContextManager.current_context()


    o = DCWithOptional()
    lt = TypeEngine.to_literal_type(DCWithOptional)
    lv = TypeEngine.to_literal(ctx, o, DCWithOptional, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, DCWithOptional)
    assert isinstance(pv, DCWithOptional)
    DataclassTransformer().assert_type(DCWithOptional, pv)

    o = DCWithOptional(string="a", dc=DC(string="b"),
                      list_dc=[DC(string="c"), DC(string="d")],
                      list_list_dc=[[DC(string="e"), DC(string="f")]],
                      list_dict_dc=[{"g": DC(string="h"), "i": DC(string="j")},
                                    {"k": DC(string="l"), "m": DC(string="n")}],
                      dict_dc={"o": DC(string="p"), "q": DC(string="r")},
                      dict_dict_dc={"s": {"t": DC(string="u"), "v": DC(string="w")}},
                      dict_list_dc={"x": [DC(string="y"), DC(string="z")],
                                    "aa": [DC(string="bb"), DC(string="cc")]})
    lt = TypeEngine.to_literal_type(DCWithOptional)
    lv = TypeEngine.to_literal(ctx, o, DCWithOptional, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, DCWithOptional)
    assert isinstance(pv, DCWithOptional)
    DataclassTransformer().assert_type(DCWithOptional, pv)


def test_pure_dataclasses_with_flyte_types(local_dummy_txt_file, local_dummy_directory):
    @dataclass
    class FlyteTypes:
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None

    @dataclass
    class NestedFlyteTypes:
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None
        flyte_types: Optional[FlyteTypes] = None
        list_flyte_types: Optional[List[FlyteTypes]] = None
        dict_flyte_types: Optional[Dict[str, FlyteTypes]] = None
        optional_flyte_types: Optional[FlyteTypes] = None

    @task
    def pass_and_return_flyte_types(nested_flyte_types: NestedFlyteTypes) -> NestedFlyteTypes:
        return nested_flyte_types

    @task
    def generate_sd() -> StructuredDataset:
        return StructuredDataset(
            uri="s3://my-s3-bucket/data/test_sd",
            file_format="parquet")

    @task
    def create_local_dir(path: str) -> FlyteDirectory:
        return FlyteDirectory(path=path)

    @task
    def create_local_dir_by_str(path: str) -> FlyteDirectory:
        return path

    @task
    def create_local_file(path: str) -> FlyteFile:
        return FlyteFile(path=path)

    @task
    def create_local_file_with_str(path: str) -> FlyteFile:
        return path

    @task
    def generate_nested_flyte_types(local_file: FlyteFile, local_dir: FlyteDirectory, sd: StructuredDataset,
                                    local_file_by_str: FlyteFile,
                                    local_dir_by_str: FlyteDirectory, ) -> NestedFlyteTypes:
        ft = FlyteTypes(
            flytefile=local_file,
            flytedir=local_dir,
            structured_dataset=sd,
        )

        return NestedFlyteTypes(
            flytefile=local_file,
            flytedir=local_dir,
            structured_dataset=sd,
            flyte_types=FlyteTypes(
                flytefile=local_file_by_str,
                flytedir=local_dir_by_str,
                structured_dataset=sd,
            ),
            list_flyte_types=[ft, ft, ft],
            dict_flyte_types={"a": ft, "b": ft, "c": ft},
        )

    @workflow
    def nested_dc_wf(txt_path: str, dir_path: str) -> NestedFlyteTypes:
        local_file = create_local_file(path=txt_path)
        local_dir = create_local_dir(path=dir_path)
        local_file_by_str = create_local_file_with_str(path=txt_path)
        local_dir_by_str = create_local_dir_by_str(path=dir_path)
        sd = generate_sd()
        nested_flyte_types = generate_nested_flyte_types(
            local_file=local_file,
            local_dir=local_dir,
            local_file_by_str=local_file_by_str,
            local_dir_by_str=local_dir_by_str,
            sd=sd
        )
        old_flyte_types = pass_and_return_flyte_types(nested_flyte_types=nested_flyte_types)
        return pass_and_return_flyte_types(nested_flyte_types=old_flyte_types)

    @task
    def get_empty_nested_type() -> NestedFlyteTypes:
        return NestedFlyteTypes()

    @workflow
    def empty_nested_dc_wf() -> NestedFlyteTypes:
        return get_empty_nested_type()

    nested_flyte_types = nested_dc_wf(txt_path=local_dummy_txt_file, dir_path=local_dummy_directory)
    DataclassTransformer().assert_type(NestedFlyteTypes, nested_flyte_types)

    empty_nested_flyte_types = empty_nested_dc_wf()
    DataclassTransformer().assert_type(NestedFlyteTypes, empty_nested_flyte_types)


def test_pure_dataclasses_with_flyte_types_get_literal_type_and_to_python_value(local_dummy_txt_file, local_dummy_directory):
    @dataclass
    class FlyteTypes:
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None

    @dataclass
    class NestedFlyteTypes:
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None
        flyte_types: Optional[FlyteTypes] = None
        list_flyte_types: Optional[List[FlyteTypes]] = None
        dict_flyte_types: Optional[Dict[str, FlyteTypes]] = None
        optional_flyte_types: Optional[FlyteTypes] = None

    ctx = FlyteContextManager.current_context()

    o = NestedFlyteTypes()

    lt = TypeEngine.to_literal_type(NestedFlyteTypes)
    lv = TypeEngine.to_literal(ctx, o, NestedFlyteTypes, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, NestedFlyteTypes)
    assert isinstance(pv, NestedFlyteTypes)
    DataclassTransformer().assert_type(NestedFlyteTypes, pv)

    ff = FlyteFile(path=local_dummy_txt_file)
    fd = FlyteDirectory(path=local_dummy_directory)
    sd = StructuredDataset(uri="s3://my-s3-bucket/data/test_sd", file_format="parquet")
    o = NestedFlyteTypes(
        flytefile=ff,
        flytedir=fd,
        structured_dataset=sd,
        flyte_types=FlyteTypes(
            flytefile=ff,
            flytedir=fd,
            structured_dataset=sd,
        ),
        list_flyte_types=[FlyteTypes(
            flytefile=ff,
            flytedir=fd,
            structured_dataset=sd,
        )],
        dict_flyte_types={
            "a": FlyteTypes(
                flytefile=ff,
                flytedir=fd,
                structured_dataset=sd,
            ),
            "b": FlyteTypes(
                flytefile=ff,
                flytedir=fd,
                structured_dataset=sd)},
        optional_flyte_types=FlyteTypes(
            flytefile=ff,
            flytedir=fd,
            structured_dataset=sd,
        ),
    )

    lt = TypeEngine.to_literal_type(NestedFlyteTypes)
    lv = TypeEngine.to_literal(ctx, o, NestedFlyteTypes, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, NestedFlyteTypes)
    assert isinstance(pv, NestedFlyteTypes)
    DataclassTransformer().assert_type(NestedFlyteTypes, pv)

## For dataclasses json mixin, it's equal to use @dataclasses_json
def test_dataclasses_json_mixin_with_python_types():
    @dataclass
    class DC(DataClassJsonMixin):
        string: Optional[str] = None

    @dataclass
    class DCWithOptional(DataClassJsonMixin):
        string: Optional[str] = None
        dc: Optional[DC] = None
        list_dc: Optional[List[DC]] = None
        list_list_dc: Optional[List[List[DC]]] = None
        dict_dc: Optional[Dict[str, DC]] = None
        dict_dict_dc: Optional[Dict[str, Dict[str, DC]]] = None
        dict_list_dc: Optional[Dict[str, List[DC]]] = None
        list_dict_dc: Optional[List[Dict[str, DC]]] = None

    @task
    def t1() -> DCWithOptional:
        return DCWithOptional(string="a", dc=DC(string="b"),
                              list_dc=[DC(string="c"), DC(string="d")],
                              list_list_dc=[[DC(string="e"), DC(string="f")]],
                              list_dict_dc=[{"g": DC(string="h"), "i": DC(string="j")},
                                            {"k": DC(string="l"), "m": DC(string="n")}],
                              dict_dc={"o": DC(string="p"), "q": DC(string="r")},
                              dict_dict_dc={"s": {"t": DC(string="u"), "v": DC(string="w")}},
                              dict_list_dc={"x": [DC(string="y"), DC(string="z")],
                                            "aa": [DC(string="bb"), DC(string="cc")]},)

    @task
    def t2() -> DCWithOptional:
        return DCWithOptional()

    output = DCWithOptional(string="a", dc=DC(string="b"),
                            list_dc=[DC(string="c"), DC(string="d")],
                            list_list_dc=[[DC(string="e"), DC(string="f")]],
                            list_dict_dc=[{"g": DC(string="h"), "i": DC(string="j")},
                                          {"k": DC(string="l"), "m": DC(string="n")}],
                            dict_dc={"o": DC(string="p"), "q": DC(string="r")},
                            dict_dict_dc={"s": {"t": DC(string="u"), "v": DC(string="w")}},
                            dict_list_dc={"x": [DC(string="y"), DC(string="z")],
                                          "aa": [DC(string="bb"), DC(string="cc")]}, )

    dc1 = t1()
    dc2 = t2()

    assert dc1 == output
    assert dc2.string is None
    assert dc2.dc is None

    DataclassTransformer().assert_type(DCWithOptional, dc1)
    DataclassTransformer().assert_type(DCWithOptional, dc2)


def test_dataclasses_json_mixin__with_python_types_get_literal_type_and_to_python_value():
    @dataclass
    class DC(DataClassJsonMixin):
        string: Optional[str] = None

    @dataclass
    class DCWithOptional(DataClassJsonMixin):
        string: Optional[str] = None
        dc: Optional[DC] = None
        list_dc: Optional[List[DC]] = None
        list_list_dc: Optional[List[List[DC]]] = None
        dict_dc: Optional[Dict[str, DC]] = None
        dict_dict_dc: Optional[Dict[str, Dict[str, DC]]] = None
        dict_list_dc: Optional[Dict[str, List[DC]]] = None
        list_dict_dc: Optional[List[Dict[str, DC]]] = None

    ctx = FlyteContextManager.current_context()


    o = DCWithOptional()
    lt = TypeEngine.to_literal_type(DCWithOptional)
    lv = TypeEngine.to_literal(ctx, o, DCWithOptional, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, DCWithOptional)
    assert isinstance(pv, DCWithOptional)
    DataclassTransformer().assert_type(DCWithOptional, pv)

    o = DCWithOptional(string="a", dc=DC(string="b"),
                      list_dc=[DC(string="c"), DC(string="d")],
                      list_list_dc=[[DC(string="e"), DC(string="f")]],
                      list_dict_dc=[{"g": DC(string="h"), "i": DC(string="j")},
                                    {"k": DC(string="l"), "m": DC(string="n")}],
                      dict_dc={"o": DC(string="p"), "q": DC(string="r")},
                      dict_dict_dc={"s": {"t": DC(string="u"), "v": DC(string="w")}},
                      dict_list_dc={"x": [DC(string="y"), DC(string="z")],
                                    "aa": [DC(string="bb"), DC(string="cc")]})
    lt = TypeEngine.to_literal_type(DCWithOptional)
    lv = TypeEngine.to_literal(ctx, o, DCWithOptional, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, DCWithOptional)
    assert isinstance(pv, DCWithOptional)
    DataclassTransformer().assert_type(DCWithOptional, pv)


def test_dataclasses_json_mixin_with_flyte_types(local_dummy_txt_file, local_dummy_directory):
    @dataclass
    class FlyteTypes(DataClassJsonMixin):
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None

    @dataclass
    class NestedFlyteTypes(DataClassJsonMixin):
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None
        flyte_types: Optional[FlyteTypes] = None
        list_flyte_types: Optional[List[FlyteTypes]] = None
        dict_flyte_types: Optional[Dict[str, FlyteTypes]] = None
        optional_flyte_types: Optional[FlyteTypes] = None

    @task
    def pass_and_return_flyte_types(nested_flyte_types: NestedFlyteTypes) -> NestedFlyteTypes:
        return nested_flyte_types

    @task
    def generate_sd() -> StructuredDataset:
        return StructuredDataset(
            uri="s3://my-s3-bucket/data/test_sd",
            file_format="parquet")

    @task
    def create_local_dir(path: str) -> FlyteDirectory:
        return FlyteDirectory(path=path)

    @task
    def create_local_dir_by_str(path: str) -> FlyteDirectory:
        return path

    @task
    def create_local_file(path: str) -> FlyteFile:
        return FlyteFile(path=path)

    @task
    def create_local_file_with_str(path: str) -> FlyteFile:
        return path

    @task
    def generate_nested_flyte_types(local_file: FlyteFile, local_dir: FlyteDirectory, sd: StructuredDataset,
                                    local_file_by_str: FlyteFile,
                                    local_dir_by_str: FlyteDirectory, ) -> NestedFlyteTypes:
        ft = FlyteTypes(
            flytefile=local_file,
            flytedir=local_dir,
            structured_dataset=sd,
        )

        return NestedFlyteTypes(
            flytefile=local_file,
            flytedir=local_dir,
            structured_dataset=sd,
            flyte_types=FlyteTypes(
                flytefile=local_file_by_str,
                flytedir=local_dir_by_str,
                structured_dataset=sd,
            ),
            list_flyte_types=[ft, ft, ft],
            dict_flyte_types={"a": ft, "b": ft, "c": ft},
        )

    @workflow
    def nested_dc_wf(txt_path: str, dir_path: str) -> NestedFlyteTypes:
        local_file = create_local_file(path=txt_path)
        local_dir = create_local_dir(path=dir_path)
        local_file_by_str = create_local_file_with_str(path=txt_path)
        local_dir_by_str = create_local_dir_by_str(path=dir_path)
        sd = generate_sd()
        # current branch -> current branch
        nested_flyte_types = generate_nested_flyte_types(
            local_file=local_file,
            local_dir=local_dir,
            local_file_by_str=local_file_by_str,
            local_dir_by_str=local_dir_by_str,
            sd=sd
        )
        old_flyte_types = pass_and_return_flyte_types(nested_flyte_types=nested_flyte_types)
        return pass_and_return_flyte_types(nested_flyte_types=old_flyte_types)

    @task
    def get_empty_nested_type() -> NestedFlyteTypes:
        return NestedFlyteTypes()

    @workflow
    def empty_nested_dc_wf() -> NestedFlyteTypes:
        return get_empty_nested_type()

    nested_flyte_types = nested_dc_wf(txt_path=local_dummy_txt_file, dir_path=local_dummy_directory)
    DataclassTransformer().assert_type(NestedFlyteTypes, nested_flyte_types)

    empty_nested_flyte_types = empty_nested_dc_wf()
    DataclassTransformer().assert_type(NestedFlyteTypes, empty_nested_flyte_types)


def test_dataclasses_json_mixin_with_flyte_types_get_literal_type_and_to_python_value(local_dummy_txt_file, local_dummy_directory):
    @dataclass
    class FlyteTypes(DataClassJsonMixin):
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None

    @dataclass
    class NestedFlyteTypes(DataClassJsonMixin):
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None
        flyte_types: Optional[FlyteTypes] = None
        list_flyte_types: Optional[List[FlyteTypes]] = None
        dict_flyte_types: Optional[Dict[str, FlyteTypes]] = None
        optional_flyte_types: Optional[FlyteTypes] = None

    ctx = FlyteContextManager.current_context()

    o = NestedFlyteTypes()

    lt = TypeEngine.to_literal_type(NestedFlyteTypes)
    lv = TypeEngine.to_literal(ctx, o, NestedFlyteTypes, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, NestedFlyteTypes)
    assert isinstance(pv, NestedFlyteTypes)
    DataclassTransformer().assert_type(NestedFlyteTypes, pv)

    ff = FlyteFile(path=local_dummy_txt_file)
    fd = FlyteDirectory(path=local_dummy_directory)
    sd = StructuredDataset(uri="s3://my-s3-bucket/data/test_sd", file_format="parquet")
    o = NestedFlyteTypes(
        flytefile=ff,
        flytedir=fd,
        structured_dataset=sd,
        flyte_types=FlyteTypes(
            flytefile=ff,
            flytedir=fd,
            structured_dataset=sd,
        ),
        list_flyte_types=[FlyteTypes(
            flytefile=ff,
            flytedir=fd,
            structured_dataset=sd,
        )],
        dict_flyte_types={
            "a": FlyteTypes(
                flytefile=ff,
                flytedir=fd,
                structured_dataset=sd,
            ),
            "b": FlyteTypes(
                flytefile=ff,
                flytedir=fd,
                structured_dataset=sd)},
        optional_flyte_types=FlyteTypes(
            flytefile=ff,
            flytedir=fd,
            structured_dataset=sd,
        ),
    )

    lt = TypeEngine.to_literal_type(NestedFlyteTypes)
    lv = TypeEngine.to_literal(ctx, o, NestedFlyteTypes, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, NestedFlyteTypes)
    assert isinstance(pv, NestedFlyteTypes)
    DataclassTransformer().assert_type(NestedFlyteTypes, pv)

# For mashumaro dataclasses mixins, it's equal to use @dataclasses only
def test_mashumaro_dataclasses_json_mixin_with_python_types():
    @dataclass
    class DC(DataClassJSONMixin):
        string: Optional[str] = None

    @dataclass
    class DCWithOptional(DataClassJSONMixin):
        string: Optional[str] = None
        dc: Optional[DC] = None
        list_dc: Optional[List[DC]] = None
        list_list_dc: Optional[List[List[DC]]] = None
        dict_dc: Optional[Dict[str, DC]] = None
        dict_dict_dc: Optional[Dict[str, Dict[str, DC]]] = None
        dict_list_dc: Optional[Dict[str, List[DC]]] = None
        list_dict_dc: Optional[List[Dict[str, DC]]] = None

    @task
    def t1() -> DCWithOptional:
        return DCWithOptional(string="a", dc=DC(string="b"),
                              list_dc=[DC(string="c"), DC(string="d")],
                              list_list_dc=[[DC(string="e"), DC(string="f")]],
                              list_dict_dc=[{"g": DC(string="h"), "i": DC(string="j")},
                                            {"k": DC(string="l"), "m": DC(string="n")}],
                              dict_dc={"o": DC(string="p"), "q": DC(string="r")},
                              dict_dict_dc={"s": {"t": DC(string="u"), "v": DC(string="w")}},
                              dict_list_dc={"x": [DC(string="y"), DC(string="z")],
                                            "aa": [DC(string="bb"), DC(string="cc")]},)

    @task
    def t2() -> DCWithOptional:
        return DCWithOptional()

    output = DCWithOptional(string="a", dc=DC(string="b"),
                   list_dc=[DC(string="c"), DC(string="d")],
                   list_list_dc=[[DC(string="e"), DC(string="f")]],
                   list_dict_dc=[{"g": DC(string="h"), "i": DC(string="j")},
                                 {"k": DC(string="l"), "m": DC(string="n")}],
                   dict_dc={"o": DC(string="p"), "q": DC(string="r")},
                   dict_dict_dc={"s": {"t": DC(string="u"), "v": DC(string="w")}},
                   dict_list_dc={"x": [DC(string="y"), DC(string="z")],
                                 "aa": [DC(string="bb"), DC(string="cc")]}, )

    dc1 = t1()
    dc2 = t2()

    assert dc1 == output
    assert dc2.string is None
    assert dc2.dc is None

    DataclassTransformer().assert_type(DCWithOptional, dc1)
    DataclassTransformer().assert_type(DCWithOptional, dc2)


def test_mashumaro_dataclasses_json_mixin_with_python_types_get_literal_type_and_to_python_value():
    @dataclass
    class DC(DataClassJSONMixin):
        string: Optional[str] = None

    @dataclass
    class DCWithOptional(DataClassJSONMixin):
        string: Optional[str] = None
        dc: Optional[DC] = None
        list_dc: Optional[List[DC]] = None
        list_list_dc: Optional[List[List[DC]]] = None
        dict_dc: Optional[Dict[str, DC]] = None
        dict_dict_dc: Optional[Dict[str, Dict[str, DC]]] = None
        dict_list_dc: Optional[Dict[str, List[DC]]] = None
        list_dict_dc: Optional[List[Dict[str, DC]]] = None

    ctx = FlyteContextManager.current_context()


    o = DCWithOptional()
    lt = TypeEngine.to_literal_type(DCWithOptional)
    lv = TypeEngine.to_literal(ctx, o, DCWithOptional, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, DCWithOptional)
    assert isinstance(pv, DCWithOptional)
    DataclassTransformer().assert_type(DCWithOptional, pv)

    o = DCWithOptional(string="a", dc=DC(string="b"),
                      list_dc=[DC(string="c"), DC(string="d")],
                      list_list_dc=[[DC(string="e"), DC(string="f")]],
                      list_dict_dc=[{"g": DC(string="h"), "i": DC(string="j")},
                                    {"k": DC(string="l"), "m": DC(string="n")}],
                      dict_dc={"o": DC(string="p"), "q": DC(string="r")},
                      dict_dict_dc={"s": {"t": DC(string="u"), "v": DC(string="w")}},
                      dict_list_dc={"x": [DC(string="y"), DC(string="z")],
                                    "aa": [DC(string="bb"), DC(string="cc")]})
    lt = TypeEngine.to_literal_type(DCWithOptional)
    lv = TypeEngine.to_literal(ctx, o, DCWithOptional, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, DCWithOptional)
    assert isinstance(pv, DCWithOptional)
    DataclassTransformer().assert_type(DCWithOptional, pv)


def test_mashumaro_dataclasses_json_mixin_with_flyte_types(local_dummy_txt_file, local_dummy_directory):
    @dataclass
    class FlyteTypes(DataClassJSONMixin):
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None

    @dataclass
    class NestedFlyteTypes(DataClassJSONMixin):
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None
        flyte_types: Optional[FlyteTypes] = None
        list_flyte_types: Optional[List[FlyteTypes]] = None
        dict_flyte_types: Optional[Dict[str, FlyteTypes]] = None
        optional_flyte_types: Optional[FlyteTypes] = None

    @task
    def pass_and_return_flyte_types(nested_flyte_types: NestedFlyteTypes) -> NestedFlyteTypes:
        return nested_flyte_types

    @task
    def generate_sd() -> StructuredDataset:
        return StructuredDataset(
            uri="s3://my-s3-bucket/data/test_sd",
            file_format="parquet")

    @task
    def create_local_dir(path: str) -> FlyteDirectory:
        return FlyteDirectory(path=path)

    @task
    def create_local_dir_by_str(path: str) -> FlyteDirectory:
        return path

    @task
    def create_local_file(path: str) -> FlyteFile:
        return FlyteFile(path=path)

    @task
    def create_local_file_with_str(path: str) -> FlyteFile:
        return path

    @task
    def generate_nested_flyte_types(local_file: FlyteFile, local_dir: FlyteDirectory, sd: StructuredDataset,
                                    local_file_by_str: FlyteFile,
                                    local_dir_by_str: FlyteDirectory, ) -> NestedFlyteTypes:
        ft = FlyteTypes(
            flytefile=local_file,
            flytedir=local_dir,
            structured_dataset=sd,
        )

        return NestedFlyteTypes(
            flytefile=local_file,
            flytedir=local_dir,
            structured_dataset=sd,
            flyte_types=FlyteTypes(
                flytefile=local_file_by_str,
                flytedir=local_dir_by_str,
                structured_dataset=sd,
            ),
            list_flyte_types=[ft, ft, ft],
            dict_flyte_types={"a": ft, "b": ft, "c": ft},
        )

    @workflow
    def nested_dc_wf(txt_path: str, dir_path: str) -> NestedFlyteTypes:
        local_file = create_local_file(path=txt_path)
        local_dir = create_local_dir(path=dir_path)
        local_file_by_str = create_local_file_with_str(path=txt_path)
        local_dir_by_str = create_local_dir_by_str(path=dir_path)
        sd = generate_sd()
        nested_flyte_types = generate_nested_flyte_types(
            local_file=local_file,
            local_dir=local_dir,
            local_file_by_str=local_file_by_str,
            local_dir_by_str=local_dir_by_str,
            sd=sd
        )
        old_flyte_types = pass_and_return_flyte_types(nested_flyte_types=nested_flyte_types)
        return pass_and_return_flyte_types(nested_flyte_types=old_flyte_types)

    @task
    def get_empty_nested_type() -> NestedFlyteTypes:
        return NestedFlyteTypes()

    @workflow
    def empty_nested_dc_wf() -> NestedFlyteTypes:
        return get_empty_nested_type()

    nested_flyte_types = nested_dc_wf(txt_path=local_dummy_txt_file, dir_path=local_dummy_directory)
    DataclassTransformer().assert_type(NestedFlyteTypes, nested_flyte_types)

    empty_nested_flyte_types = empty_nested_dc_wf()
    DataclassTransformer().assert_type(NestedFlyteTypes, empty_nested_flyte_types)


def test_mashumaro_dataclasses_json_mixin_with_flyte_types_get_literal_type_and_to_python_value(local_dummy_txt_file, local_dummy_directory):
    @dataclass
    class FlyteTypes(DataClassJSONMixin):
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None

    @dataclass
    class NestedFlyteTypes(DataClassJSONMixin):
        flytefile: Optional[FlyteFile] = None
        flytedir: Optional[FlyteDirectory] = None
        structured_dataset: Optional[StructuredDataset] = None
        flyte_types: Optional[FlyteTypes] = None
        list_flyte_types: Optional[List[FlyteTypes]] = None
        dict_flyte_types: Optional[Dict[str, FlyteTypes]] = None
        optional_flyte_types: Optional[FlyteTypes] = None

    ctx = FlyteContextManager.current_context()

    o = NestedFlyteTypes()

    lt = TypeEngine.to_literal_type(NestedFlyteTypes)
    lv = TypeEngine.to_literal(ctx, o, NestedFlyteTypes, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, NestedFlyteTypes)
    assert isinstance(pv, NestedFlyteTypes)
    DataclassTransformer().assert_type(NestedFlyteTypes, pv)

    ff = FlyteFile(path=local_dummy_txt_file)
    fd = FlyteDirectory(path=local_dummy_directory)
    sd = StructuredDataset(uri="s3://my-s3-bucket/data/test_sd", file_format="parquet")
    o = NestedFlyteTypes(
        flytefile=ff,
        flytedir=fd,
        structured_dataset=sd,
        flyte_types=FlyteTypes(
            flytefile=ff,
            flytedir=fd,
            structured_dataset=sd,
        ),
        list_flyte_types=[FlyteTypes(
            flytefile=ff,
            flytedir=fd,
            structured_dataset=sd,
        )],
        dict_flyte_types={
            "a": FlyteTypes(
                flytefile=ff,
                flytedir=fd,
                structured_dataset=sd,
            ),
            "b": FlyteTypes(
                flytefile=ff,
                flytedir=fd,
                structured_dataset=sd)},
        optional_flyte_types=FlyteTypes(
            flytefile=ff,
            flytedir=fd,
            structured_dataset=sd,
        ),
    )

    lt = TypeEngine.to_literal_type(NestedFlyteTypes)
    lv = TypeEngine.to_literal(ctx, o, NestedFlyteTypes, lt)
    assert lv is not None
    pv = TypeEngine.to_python_value(ctx, lv, NestedFlyteTypes)
    assert isinstance(pv, NestedFlyteTypes)
    DataclassTransformer().assert_type(NestedFlyteTypes, pv)
