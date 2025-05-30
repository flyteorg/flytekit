import os
import tempfile
import typing
from collections import OrderedDict
from pathlib import Path
import mock
import google.cloud.bigquery
import pytest
from fsspec.utils import get_protocol
from typing_extensions import Annotated

import flytekit.configuration
from flytekit.configuration import Image, ImageConfig
from flytekit.core.base_task import kwtypes
from flytekit.core.context_manager import ExecutionState, FlyteContext, FlyteContextManager
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.task import task
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import workflow
from flytekit.lazy_import.lazy_module import is_imported
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata, Literal
from flytekit.models.types import LiteralType, SchemaType, SimpleType, StructuredDatasetType
from flytekit.tools.translator import get_serializable
from flytekit.types.structured.structured_dataset import (
    PARQUET,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
    convert_schema_type_to_structured_dataset_type,
    extract_cols_and_format,
)

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")

my_cols = kwtypes(w=typing.Dict[str, typing.Dict[str, int]],
                  x=typing.List[typing.List[int]], y=int, z=str)

fields = [("some_int", pa.int32()), ("some_string", pa.string())]
arrow_schema = pa.schema(fields)

serialization_settings = flytekit.configuration.SerializationSettings(
    project="proj",
    domain="dom",
    version="123",
    image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
    env={},
)
df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})


def test_protocol():
    assert get_protocol("s3://my-s3-bucket/file") == "s3"
    assert get_protocol("/file") == "file"


def generate_pandas() -> pd.DataFrame:
    return pd.DataFrame({"name": ["Tom", "Joseph"], "age": [20, 22]})


@pytest.fixture
def local_tmp_pqt_file():
    df = generate_pandas()

    # Create a temporary parquet file
    with tempfile.NamedTemporaryFile(delete=False, mode="w+b", suffix=".parquet") as pqt_file:
        pqt_path = pqt_file.name
        df.to_parquet(pqt_path)

    yield pqt_path

    # Cleanup
    Path(pqt_path).unlink(missing_ok=True)


def test_formats_make_sense():
    @task
    def t1(a: pd.DataFrame) -> pd.DataFrame:
        print(a)
        return generate_pandas()

    # this should be an empty string format
    assert t1.interface.outputs["o0"].type.structured_dataset_type.format == ""
    assert t1.interface.inputs["a"].type.structured_dataset_type.format == ""

    ctx = FlyteContextManager.current_context()
    with FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.new_execution_state().with_params(
                mode=ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION)
        )
    ):
        result = t1(a=generate_pandas())
        val = result.val.scalar.value
        assert val.metadata.structured_dataset_type.format == "parquet"


def test_setting_of_unset_formats():
    custom = Annotated[StructuredDataset, "parquet"]
    example = custom(dataframe=df, uri="/path")
    # It's okay that the annotation is not used here yet.
    assert example.file_format == ""

    @task
    def t2(path: str) -> StructuredDataset:
        sd = StructuredDataset(dataframe=df, uri=path)
        return sd

    @workflow
    def wf(path: str) -> StructuredDataset:
        return t2(path=path)

    with tempfile.TemporaryDirectory() as tmp_dir:
        fname = os.path.join(tmp_dir, "somewhere")
        res = wf(path=fname)
    # Now that it's passed through an encoder however, it should be set.
    assert res.file_format == "parquet"


def test_types_pandas():
    pt = pd.DataFrame
    lt = TypeEngine.to_literal_type(pt)
    assert lt.structured_dataset_type is not None
    assert lt.structured_dataset_type.format == ""
    assert lt.structured_dataset_type.columns == []

    pt = Annotated[pd.DataFrame, "csv"]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.structured_dataset_type.format == "csv"


def test_annotate_extraction():
    xyz = Annotated[pd.DataFrame, "myformat"]
    a, b, c, d = extract_cols_and_format(xyz)
    assert a is pd.DataFrame
    assert b is None
    assert c == "myformat"
    assert d is None

    a, b, c, d = extract_cols_and_format(pd.DataFrame)
    assert a is pd.DataFrame
    assert b is None
    assert c == ""
    assert d is None


def test_types_annotated():
    pt = Annotated[pd.DataFrame, my_cols]
    lt = TypeEngine.to_literal_type(pt)
    assert len(lt.structured_dataset_type.columns) == 4
    assert lt.structured_dataset_type.columns[0].literal_type.map_value_type.map_value_type.simple == SimpleType.INTEGER
    assert (
        lt.structured_dataset_type.columns[1].literal_type.collection_type.collection_type.simple == SimpleType.INTEGER
    )
    assert lt.structured_dataset_type.columns[2].literal_type.simple == SimpleType.INTEGER
    assert lt.structured_dataset_type.columns[3].literal_type.simple == SimpleType.STRING

    pt = Annotated[pd.DataFrame, PARQUET, arrow_schema]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.structured_dataset_type.external_schema_type == "arrow"
    assert "some_string" in str(
        lt.structured_dataset_type.external_schema_bytes)

    pt = Annotated[pd.DataFrame, kwtypes(a=None)]
    with pytest.raises(AssertionError, match="type None is currently not supported by StructuredDataset"):
        TypeEngine.to_literal_type(pt)


def test_types_sd():
    pt = StructuredDataset
    lt = TypeEngine.to_literal_type(pt)
    assert lt.structured_dataset_type is not None

    pt = Annotated[StructuredDataset, my_cols]
    lt = TypeEngine.to_literal_type(pt)
    assert len(lt.structured_dataset_type.columns) == 4

    pt = Annotated[StructuredDataset, my_cols, "csv"]
    lt = TypeEngine.to_literal_type(pt)
    assert len(lt.structured_dataset_type.columns) == 4
    assert lt.structured_dataset_type.format == "csv"

    pt = Annotated[StructuredDataset, {}, "csv"]
    lt = TypeEngine.to_literal_type(pt)
    assert len(lt.structured_dataset_type.columns) == 0
    assert lt.structured_dataset_type.format == "csv"


def test_retrieving():
    assert StructuredDatasetTransformerEngine.get_encoder(
        pd.DataFrame, "file", PARQUET) is not None
    # Asking for a generic means you're okay with any one registered for that
    # type assuming there's just one.
    assert StructuredDatasetTransformerEngine.get_encoder(
        pd.DataFrame, "file", ""
    ) is StructuredDatasetTransformerEngine.get_encoder(pd.DataFrame, "file", PARQUET)

    class TempEncoder(StructuredDatasetEncoder):
        def __init__(self, protocol):
            super().__init__(MyDF, protocol)

        def encode(self):
            ...

    StructuredDatasetTransformerEngine.register(
        TempEncoder("gs"), default_for_type=False)
    with pytest.raises(ValueError):
        StructuredDatasetTransformerEngine.register(
            TempEncoder("gs://"), default_for_type=False)

    with pytest.raises(ValueError, match="Use None instead"):
        e = TempEncoder("")
        e._protocol = ""
        StructuredDatasetTransformerEngine.register(e)

    class TempEncoder:
        pass

    with pytest.raises(TypeError, match="We don't support this type of handler"):
        StructuredDatasetTransformerEngine.register(
            TempEncoder, default_for_type=False)


def test_to_literal():
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(pd.DataFrame)
    df = generate_pandas()

    fdt = StructuredDatasetTransformerEngine()

    lit = fdt.to_literal(ctx, df, python_type=pd.DataFrame, expected=lt)
    assert lit.scalar.structured_dataset.metadata.structured_dataset_type.format == PARQUET
    assert lit.scalar.structured_dataset.metadata.structured_dataset_type.format == PARQUET

    sd_with_literal_and_df = StructuredDataset(df)
    sd_with_literal_and_df._literal_sd = lit

    with pytest.raises(ValueError, match="Shouldn't have specified both literal"):
        fdt.to_literal(
            ctx,
            sd_with_literal_and_df,
            python_type=StructuredDataset,
            expected=lt)

    sd_with_nothing = StructuredDataset()
    with pytest.raises(ValueError, match="If dataframe is not specified"):
        fdt.to_literal(
            ctx,
            sd_with_nothing,
            python_type=StructuredDataset,
            expected=lt)

    sd_with_uri = StructuredDataset(uri="s3://some/extant/df.parquet")

    lt = TypeEngine.to_literal_type(
        Annotated[StructuredDataset, {}, "new-df-format"])
    lit = fdt.to_literal(
        ctx,
        sd_with_uri,
        python_type=StructuredDataset,
        expected=lt)
    assert lit.scalar.structured_dataset.uri == "s3://some/extant/df.parquet"
    assert lit.scalar.structured_dataset.metadata.structured_dataset_type.format == "new-df-format"


class MyDF(pd.DataFrame):
    ...


def test_fill_in_literal_type():
    class TempEncoder(StructuredDatasetEncoder):
        def __init__(self, fmt: str):
            super().__init__(MyDF, "tmpfs://", supported_format=fmt)

        def encode(
            self,
            ctx: FlyteContext,
            structured_dataset: StructuredDataset,
            structured_dataset_type: StructuredDatasetType,
        ) -> literals.StructuredDataset:
            return literals.StructuredDataset(uri="")

    default_encoder = TempEncoder("myavro")
    StructuredDatasetTransformerEngine.register(
        default_encoder, default_for_type=True)
    lt = TypeEngine.to_literal_type(MyDF)
    assert lt.structured_dataset_type.format == ""

    ctx = FlyteContextManager.current_context()
    fdt = StructuredDatasetTransformerEngine()
    sd = StructuredDataset(dataframe=MyDF())
    l = fdt.to_literal(ctx, sd, MyDF, lt)
    # Test that the literal type is filled in even though the encode function
    # above doesn't do it.
    assert l.scalar.structured_dataset.metadata.structured_dataset_type.format == "myavro"

    # Test that looking up encoders/decoders falls back to the ""
    # encoder/decoder
    empty_format_temp_encoder = TempEncoder("")
    StructuredDatasetTransformerEngine.register(
        empty_format_temp_encoder, default_for_type=False)

    res = StructuredDatasetTransformerEngine.get_encoder(
        MyDF, "tmpfs", "rando")
    assert res is empty_format_temp_encoder


def test_slash_register():
    class TempEncoder(StructuredDatasetEncoder):
        def __init__(self, fmt: str):
            super().__init__(MyDF, None, supported_format=fmt)

        def encode(
            self,
            ctx: FlyteContext,
            structured_dataset: StructuredDataset,
            structured_dataset_type: StructuredDatasetType,
        ) -> literals.StructuredDataset:
            return literals.StructuredDataset(uri="")

    # Check that registering with a / triggers the file protocol instead.
    StructuredDatasetTransformerEngine.register(TempEncoder("/"))
    res = StructuredDatasetTransformerEngine.get_encoder(MyDF, "file", "/")
    # Test that the one we got was registered under fsspec
    assert res is StructuredDatasetTransformerEngine.ENCODERS[MyDF].get("fsspec")[
        "/"]
    assert res is not None


def test_sd():
    sd = StructuredDataset(dataframe="hi")
    sd.uri = "my uri"
    assert sd.file_format == ""

    with pytest.raises(ValueError, match="No dataframe type set"):
        sd.all()

    with pytest.raises(ValueError, match="No dataframe type set."):
        sd.iter()

    class MockPandasDecodingHandlers(StructuredDatasetDecoder):
        def decode(
            self,
            ctx: FlyteContext,
            flyte_value: literals.StructuredDataset,
            current_task_metadata: StructuredDatasetMetadata,
        ) -> typing.Union[typing.Generator[pd.DataFrame, None, None]]:
            yield pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})

    StructuredDatasetTransformerEngine.register(
        MockPandasDecodingHandlers(pd.DataFrame, "tmpfs"), default_for_type=False
    )
    sd = StructuredDataset()
    sd._literal_sd = literals.StructuredDataset(
        uri="tmpfs://somewhere", metadata=StructuredDatasetMetadata(StructuredDatasetType(format=""))
    )
    assert isinstance(sd.open(pd.DataFrame).iter(), typing.Generator)

    with pytest.raises(ValueError):
        sd.open(pd.DataFrame).all()

    class MockPandasDecodingHandlers(StructuredDatasetDecoder):
        def decode(
            self,
            ctx: FlyteContext,
            flyte_value: literals.StructuredDataset,
            current_task_metadata: StructuredDatasetMetadata,
        ) -> pd.DataFrame:
            return pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})

    StructuredDatasetTransformerEngine.register(
        MockPandasDecodingHandlers(pd.DataFrame, "tmpfs"), default_for_type=False, override=True
    )
    sd = StructuredDataset()
    sd._literal_sd = literals.StructuredDataset(
        uri="tmpfs://somewhere", metadata=StructuredDatasetMetadata(StructuredDatasetType(format=""))
    )

    with pytest.raises(ValueError):
        sd.open(pd.DataFrame).iter()


def test_convert_schema_type_to_structured_dataset_type():
    schema_ct = SchemaType.SchemaColumn.SchemaColumnType
    assert convert_schema_type_to_structured_dataset_type(
        schema_ct.INTEGER) == SimpleType.INTEGER
    assert convert_schema_type_to_structured_dataset_type(
        schema_ct.FLOAT) == SimpleType.FLOAT
    assert convert_schema_type_to_structured_dataset_type(
        schema_ct.STRING) == SimpleType.STRING
    assert convert_schema_type_to_structured_dataset_type(
        schema_ct.DATETIME) == SimpleType.DATETIME
    assert convert_schema_type_to_structured_dataset_type(
        schema_ct.DURATION) == SimpleType.DURATION
    assert convert_schema_type_to_structured_dataset_type(
        schema_ct.BOOLEAN) == SimpleType.BOOLEAN
    with pytest.raises(AssertionError, match="Unrecognized SchemaColumnType"):
        convert_schema_type_to_structured_dataset_type(int)

    with pytest.raises(AssertionError, match="Unrecognized SchemaColumnType"):
        convert_schema_type_to_structured_dataset_type(20)


def test_to_python_value_with_incoming_columns():
    # make a literal with a type that has two columns
    original_type = Annotated[pd.DataFrame, kwtypes(name=str, age=int)]
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(original_type)
    df = generate_pandas()
    fdt = StructuredDatasetTransformerEngine()
    lit = fdt.to_literal(ctx, df, python_type=original_type, expected=lt)
    assert len(
        lit.scalar.structured_dataset.metadata.structured_dataset_type.columns) == 2

    # declare a new type that only has one column
    # get the dataframe, make sure it has the column that was asked for.
    subset_sd_type = Annotated[StructuredDataset, kwtypes(age=int)]
    sd = fdt.to_python_value(ctx, lit, subset_sd_type)
    assert sd.metadata.structured_dataset_type.columns[0].name == "age"
    sub_df = sd.open(pd.DataFrame).all()
    assert sub_df.shape[1] == 1

    # check when columns are not specified, should pull both and add column
    # information.
    sd = fdt.to_python_value(ctx, lit, StructuredDataset)
    assert len(sd.metadata.structured_dataset_type.columns) == 2

    # should also work if subset type is just an annotated pd.DataFrame
    subset_pd_type = Annotated[pd.DataFrame, kwtypes(age=int)]
    sub_df = fdt.to_python_value(ctx, lit, subset_pd_type)
    assert sub_df.shape[1] == 1


def test_to_python_value_without_incoming_columns():
    # make a literal with a type with no columns
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(pd.DataFrame)
    df = generate_pandas()
    fdt = StructuredDatasetTransformerEngine()
    lit = fdt.to_literal(ctx, df, python_type=pd.DataFrame, expected=lt)
    assert len(
        lit.scalar.structured_dataset.metadata.structured_dataset_type.columns) == 0

    # declare a new type that only has one column
    # get the dataframe, make sure it has the column that was asked for.
    subset_sd_type = Annotated[StructuredDataset, kwtypes(age=int)]
    sd = fdt.to_python_value(ctx, lit, subset_sd_type)
    assert sd.metadata.structured_dataset_type.columns[0].name == "age"
    sub_df = sd.open(pd.DataFrame).all()
    assert sub_df.shape[1] == 1

    # check when columns are not specified, should pull both and add column information.
    # todo: see the todos in the open_as, and iter_as functions in StructuredDatasetTransformerEngine
    # we have to recreate the literal because the test case above filled in
    # the metadata
    lit = fdt.to_literal(ctx, df, python_type=pd.DataFrame, expected=lt)
    sd = fdt.to_python_value(ctx, lit, StructuredDataset)
    assert sd.metadata.structured_dataset_type.columns == []
    sub_df = sd.open(pd.DataFrame).all()
    assert sub_df.shape[1] == 2

    # should also work if subset type is just an annotated pd.DataFrame
    lit = fdt.to_literal(ctx, df, python_type=pd.DataFrame, expected=lt)
    subset_pd_type = Annotated[pd.DataFrame, kwtypes(age=int)]
    sub_df = fdt.to_python_value(ctx, lit, subset_pd_type)
    assert sub_df.shape[1] == 1


def test_format_correct():
    class TempEncoder(StructuredDatasetEncoder):
        def __init__(self):
            super().__init__(pd.DataFrame, "/", "avro")

        def encode(
            self,
            ctx: FlyteContext,
            structured_dataset: StructuredDataset,
            structured_dataset_type: StructuredDatasetType,
        ) -> literals.StructuredDataset:
            return literals.StructuredDataset(
                uri="/tmp/avro", metadata=StructuredDatasetMetadata(structured_dataset_type)
            )

    ctx = FlyteContextManager.current_context()
    df = pd.DataFrame({"name": ["Tom", "Joseph"], "age": [20, 22]})

    annotated_sd_type = Annotated[StructuredDataset,
                                  "avro", kwtypes(name=str, age=int)]
    df_literal_type = TypeEngine.to_literal_type(annotated_sd_type)
    assert df_literal_type.structured_dataset_type is not None
    assert len(df_literal_type.structured_dataset_type.columns) == 2
    assert df_literal_type.structured_dataset_type.columns[0].name == "name"
    assert df_literal_type.structured_dataset_type.columns[0].literal_type.simple is not None
    assert df_literal_type.structured_dataset_type.columns[1].name == "age"
    assert df_literal_type.structured_dataset_type.columns[1].literal_type.simple is not None
    assert df_literal_type.structured_dataset_type.format == "avro"

    sd = annotated_sd_type(df)
    with pytest.raises(ValueError, match="Failed to find a handler"):
        TypeEngine.to_literal(
            ctx,
            sd,
            python_type=annotated_sd_type,
            expected=df_literal_type)

    StructuredDatasetTransformerEngine.register(
        TempEncoder(), default_for_type=False)
    sd2 = annotated_sd_type(df)
    sd_literal = TypeEngine.to_literal(
        ctx,
        sd2,
        python_type=annotated_sd_type,
        expected=df_literal_type)
    assert sd_literal.scalar.structured_dataset.metadata.structured_dataset_type.format == "avro"

    @task
    def t1() -> Annotated[StructuredDataset, "avro"]:
        return StructuredDataset(dataframe=df)

    assert t1().file_format == "avro"


def test_protocol_detection():
    # We've don't register defaults to the transformer engine
    assert pd.DataFrame not in StructuredDatasetTransformerEngine.DEFAULT_PROTOCOLS
    e = StructuredDatasetTransformerEngine()
    ctx = FlyteContextManager.current_context()
    protocol = e._protocol_from_type_or_prefix(ctx, pd.DataFrame)
    assert protocol == "file"

    with tempfile.TemporaryDirectory() as tmp_dir:
        fs = FileAccessProvider(
            local_sandbox_dir=tmp_dir,
            raw_output_prefix="s3://fdsa")
        ctx2 = ctx.with_file_access(fs).build()
        protocol = e._protocol_from_type_or_prefix(ctx2, pd.DataFrame)
        assert protocol == "s3"

        protocol = e._protocol_from_type_or_prefix(
            ctx2, pd.DataFrame, "bq://foo")
        assert protocol == "bq"


def test_register_renderers():
    class DummyRenderer:
        def to_html(self, input: str) -> str:
            return "hello " + input

    renderers = StructuredDatasetTransformerEngine.Renderers
    StructuredDatasetTransformerEngine.register_renderer(str, DummyRenderer())
    assert renderers[str].to_html("flyte") == "hello flyte"
    assert pd.DataFrame in renderers
    assert pa.Table in renderers

    with pytest.raises(NotImplementedError, match="Could not find a renderer for <class 'int'> in"):
        StructuredDatasetTransformerEngine().to_html(
            FlyteContextManager.current_context(), 3, int)


def test_list_of_annotated():
    WineDataset = Annotated[
        StructuredDataset,
        kwtypes(
            alcohol=float,
            malic_acid=float,
        ),
    ]

    @task
    def no_op(data: WineDataset) -> typing.List[WineDataset]:
        return [data]


class PrivatePandasToBQEncodingHandlers(StructuredDatasetEncoder):
    def __init__(self):
        super().__init__(pd.DataFrame, "bq", supported_format="")

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        return literals.StructuredDataset(
            uri=typing.cast(str, structured_dataset.uri), metadata=StructuredDatasetMetadata(structured_dataset_type)
        )


def test_reregister_encoder():
    # Test that lazy import can run after a user has already registered a custom handler.
    # The default handlers don't have override=True (and should not) but the
    # call should not fail.
    dir(google.cloud.bigquery)
    assert is_imported("google.cloud.bigquery")

    StructuredDatasetTransformerEngine.register(
        PrivatePandasToBQEncodingHandlers(), default_format_for_type=False, override=True
    )
    TypeEngine.lazy_import_transformers()

    sd = StructuredDataset(dataframe=pd.DataFrame(
        {"a": [1, 2], "b": [3, 4]}), uri="bq://blah", file_format="bq")

    ctx = FlyteContextManager.current_context()

    df_literal_type = TypeEngine.to_literal_type(pd.DataFrame)

    TypeEngine.to_literal(
        ctx,
        sd,
        python_type=pd.DataFrame,
        expected=df_literal_type)


def test_default_args_task():
    default_val = pd.DataFrame({"name": ["Aegon"], "age": [27]})
    input_val = generate_pandas()

    @task
    def t1(a: pd.DataFrame = default_val) -> pd.DataFrame:
        return a

    @workflow
    def wf_no_input() -> pd.DataFrame:
        return t1()

    @workflow
    def wf_with_input() -> pd.DataFrame:
        return t1(a=input_val)

    wf_no_input_spec = get_serializable(
        OrderedDict(), serialization_settings, wf_no_input)
    wf_with_input_spec = get_serializable(
        OrderedDict(), serialization_settings, wf_with_input)

    assert wf_no_input_spec.template.nodes[0].inputs[
        0
    ].binding.value.structured_dataset.metadata == StructuredDatasetMetadata(
        structured_dataset_type=StructuredDatasetType(
            format="parquet",
        ),
    )
    assert wf_with_input_spec.template.nodes[0].inputs[
        0
    ].binding.value.structured_dataset.metadata == StructuredDatasetMetadata(
        structured_dataset_type=StructuredDatasetType(
            format="parquet",
        ),
    )

    assert wf_no_input_spec.template.interface.outputs["o0"].type == LiteralType(
        structured_dataset_type=StructuredDatasetType()
    )
    assert wf_with_input_spec.template.interface.outputs["o0"].type == LiteralType(
        structured_dataset_type=StructuredDatasetType()
    )

    pd.testing.assert_frame_equal(wf_no_input(), default_val)
    pd.testing.assert_frame_equal(wf_with_input(), input_val)


def test_read_sd_from_local_uri(local_tmp_pqt_file):

    @task
    def read_sd_from_uri(uri: str) -> pd.DataFrame:
        sd = StructuredDataset(uri=uri, file_format="parquet")
        df = sd.open(pd.DataFrame).all()

        return df

    @workflow
    def read_sd_from_local_uri(uri: str) -> pd.DataFrame:
        df = read_sd_from_uri(uri=uri)

        return df

    df = generate_pandas()

    # Read sd from local uri
    df_local = read_sd_from_local_uri(uri=local_tmp_pqt_file)
    pd.testing.assert_frame_equal(df, df_local)


@mock.patch("flytekit.remote.remote_fs.FlytePathResolver")
@mock.patch("flytekit.types.structured.structured_dataset.StructuredDatasetTransformerEngine.get_encoder")
def test_modify_literal_uris_call(mock_get_encoder, mock_resolver):

    ctx = FlyteContextManager.current_context()

    sd = StructuredDataset(dataframe=pd.DataFrame(
        {"a": [1, 2], "b": [3, 4]}), uri="bq://blah", file_format="bq")

    def mock_resolve_remote_path(flyte_uri: str) -> typing.Optional[str]:
        if flyte_uri == "bq://blah":
            return "bq://blah/blah/blah"
        return ""

    mock_resolver.resolve_remote_path.side_effect = mock_resolve_remote_path
    mock_resolver.protocol = "bq"

    dummy_encoder = mock.MagicMock()
    sd_model = literals.StructuredDataset(uri="bq://blah", metadata=StructuredDatasetMetadata(StructuredDatasetType(format="parquet")))
    dummy_encoder.encode.return_value = sd_model

    mock_get_encoder.return_value = dummy_encoder

    sdte = StructuredDatasetTransformerEngine()
    lt = LiteralType(
        structured_dataset_type=StructuredDatasetType()
    )

    lit = sdte.encode(ctx, sd, df_type=pd.DataFrame, protocol="bq", format="parquet", structured_literal_type=lt)
    assert lit.scalar.structured_dataset.uri == "bq://blah/blah/blah"

def test_structured_dataset_pickleable():
    import pickle

    upstream_output = Literal(
        scalar=literals.Scalar(
            structured_dataset=StructuredDataset(
                dataframe=pd.DataFrame({"a": [1, 2], "b": [3, 4]}),
                uri="bq://test_uri",
                metadata=StructuredDatasetMetadata(
                    structured_dataset_type=StructuredDatasetType(
                        columns=[
                            StructuredDatasetType.DatasetColumn(
                                name="a",
                                literal_type=LiteralType(simple=SimpleType.INTEGER)
                            ),
                            StructuredDatasetType.DatasetColumn(
                                name="b",
                                literal_type=LiteralType(simple=SimpleType.INTEGER)
                            )
                        ],
                        format="parquet"
                    )
                )
            )
        )
    )

    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(),
        upstream_output,
        StructuredDataset
    )

    pickled_input = pickle.dumps(downstream_input)
    unpickled_input = pickle.loads(pickled_input)

    assert downstream_input == unpickled_input
