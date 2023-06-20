import enum
import typing
from collections import OrderedDict
from dataclasses import dataclass
from typing import Annotated, Dict, List

import pandas as pd
from dataclasses_json import dataclass_json

from flytekit import StructuredDataset, current_context, kwtypes
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.tools.translator import get_serializable
from flytekit.types.directory import FlyteDirectory
from flytekit.types.structured.structured_dataset import PARQUET
from flytekit.models.literals import Binding

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


def test_dataclass():
    @dataclass_json
    @dataclass
    class AppParams(object):
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


def test_dataclass_no_json():
    @dataclass
    class AppParams(object):
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


def test_dataclass_complex_types():
    class AnEnum(enum.Enum):
        ONE = "one"
        TWO = "two"

    @dataclass
    class AppParams:
        snapshotDate: str
        region: str
        preprocess: bool
        listKeys: List[str]

    class Foo(object):
        def __init__(self, number: int):
            self.number = number

    @dataclass
    class ComplexTypes:
        app_params: AppParams
        enum: AnEnum
        dir: FlyteDirectory
        dataset: Annotated[StructuredDataset, kwtypes(column=str), PARQUET]
        pickle_data: Foo

    @task
    def t1() -> ComplexTypes:
        ap = ComplexTypes(
            app_params=AppParams(snapshotDate="4/5/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"]),
            enum=AnEnum.ONE,
            dir=FlyteDirectory(path=current_context().working_directory),
            dataset=StructuredDataset(dataframe=pd.DataFrame(dict(column=["foo", "bar"]))),
            pickle_data=Foo(1),
        )
        return ap

    @workflow
    def wf() -> ComplexTypes:
        return t1()

    res = wf()
    assert res.app_params.region == "us-west-3"


def test_dataclass_dict():
    @dataclass
    class AppParams(object):
        snapshotDate: str
        region: str
        preprocess: bool
        listKeys: List[str]

    @task
    def first(params: Dict[str, AppParams]) -> AppParams:
        return params["first"]

    @workflow
    def wf(params: Dict[str, AppParams]) -> AppParams:
        return first(params=params)

    res = wf(
        params={
            "first": AppParams(snapshotDate="4/5/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"]),
            "later": AppParams(snapshotDate="4/6/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"]),
        }
    )
    assert res.snapshotDate == "4/5/2063"


def test_generalization():
    @dataclass_json
    @dataclass
    class MyContainer(object):
        snapshot_date_str: bool
        list_keys: List[str]

    @task
    def get_true() -> bool:
        return True

    @task
    def get_strs() -> List[str]:
        return ["hello", "world"]

    @task
    def consume_and_print(a: MyContainer):
        print(a)

    @workflow
    def wf() -> MyContainer:
        b = get_true()
        l = get_strs()
        x = consume_and_print(a=MyContainer(snapshot_date_str=b, list_keys=l))
        other_task(b=x.snapshot_date_str)
        return MyContainer(snapshot_date_str=b, list_keys=l)

    @workflow
    def example_for_map() -> typing.Dict[str, List[str]]:
        a = get_strs()
        b = get_strs()
        consume_and_print(a={"first_strings": a, "second_strings": b})
        return {"first_strings": a, "second_strings": b}


def test_dataclass_more_complex():
    @dataclass
    class MainParams:
        region: str
        preprocess: bool
        listKeys: List[str]

    @dataclass
    class ComplexTypes:
        app_params: MainParams
        dir: FlyteDirectory
        dataset: Annotated[StructuredDataset, kwtypes(column=str), PARQUET]

    @task
    def get_df_task() -> Annotated[StructuredDataset, kwtypes(column=str), PARQUET]:
        df = pd.DataFrame(dict(column=["foo", "bar"]))
        return StructuredDataset(df)

    @task
    def get_dir_task() -> FlyteDirectory:
        return FlyteDirectory(path=current_context().working_directory)

    @task
    def t1() -> ComplexTypes:
        df = pd.DataFrame(dict(column=["one", "two"]))
        ap = ComplexTypes(
            app_params=MainParams(region="us-west-3", preprocess=False, listKeys=["a", "b"]),
            dir=FlyteDirectory(path=current_context().working_directory),
            dataset=StructuredDataset(dataframe=df),
        )
        return ap

    @task
    def consume(a: ComplexTypes):
        print(a)

    @workflow
    def wf():
        sd = get_df_task()
        d = get_dir_task()
        consume(
            a=ComplexTypes(
                app_params=MainParams(region="us-west-3", preprocess=False, listKeys=["a", "b"]), dir=d, dataset=sd
            )
        )
        consume(a=t1())

    # wf()

    od = OrderedDict()
    wf.compile()
    wf_spec = get_serializable(od, serialization_settings, wf)
    print(wf_spec.template)
    assert len(wf_spec.template.nodes) == 5

    n2 = wf_spec.template.nodes[2]
    assert len(n2.inputs) == 1
    assert isinstance(n2.inputs[0], Binding)
    assert n2.inputs[0].var == "a"
    assert len(n2.inputs[0].binding.map.bindings) == 3
    assert n2.inputs[0].binding.map.bindings["app_params"].map.bindings["region"].scalar.primitive.string_value == \
    "us-west-3"
    assert n2.inputs[0].binding.map.bindings["app_params"].map.bindings["preprocess"].scalar.primitive.boolean is False
    assert n2.inputs[0].binding.map.bindings["app_params"].map.bindings["listKeys"].collection.bindings[
        0].scalar.primitive.string_value == "a"

    assert n2.inputs[0].binding.map.bindings["dir"].promise.node_id == "n1"
    assert n2.inputs[0].binding.map.bindings["dir"].promise.var == "o0"

    assert n2.inputs[0].binding.map.bindings["dir"].promise.node_id == "n1"
    assert n2.inputs[0].binding.map.bindings["dir"].promise.var == "o0"

    assert n2.inputs[0].binding.map.bindings["dataset"].promise.node_id == "n0"
    assert n2.inputs[0].binding.map.bindings["dataset"].promise.var == "o0"


