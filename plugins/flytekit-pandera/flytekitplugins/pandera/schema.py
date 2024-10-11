import typing
from dataclasses import dataclass
from typing import Type, TYPE_CHECKING

from flytekit import Deck, FlyteContext, lazy_module
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.models import literals
from flytekit.models.literals import Literal, Scalar, Schema
from flytekit.models.types import LiteralType, SchemaType
from flytekit.types.schema import SchemaFormat, SchemaOpenMode
from flytekit.types.structured import StructuredDataset
from flytekit.types.structured import structured_dataset
from flytekit.types.schema.types_pandas import PandasSchemaWriter

from .config import PanderaValidationConfig
from .renderer import PanderaReportRenderer


if TYPE_CHECKING:
    import pandas
    import pandera
else:
    pandas = lazy_module("pandas")
    pandera = lazy_module("pandera")


T = typing.TypeVar("T")


class PanderaTransformer(TypeTransformer[pandera.typing.DataFrame]):
    _SUPPORTED_TYPES: typing.Dict[type, SchemaType.SchemaColumn.SchemaColumnType] = (
        structured_dataset.get_supported_types()
    )

    def __init__(self):
        super().__init__("Pandera Transformer", pandera.typing.DataFrame)  # type: ignore

    def _pandera_schema(self, t: Type[pandera.typing.DataFrame]):
        config = PanderaValidationConfig()
        if typing.get_origin(t) is typing.Annotated:
            t, *args = typing.get_args(t)
            # get pandera config
            for arg in args:
                if isinstance(arg, PanderaValidationConfig):
                    config = arg
                    break

        try:
            type_args = typing.get_args(t)
        except AttributeError:
            # for python < 3.8
            type_args = getattr(t, "__args__", None)

        if type_args:
            schema_model, *_ = type_args
            schema = schema_model.to_schema()
        else:
            schema = pandera.DataFrameSchema()  # type: ignore
        return schema, config

    @staticmethod
    def _get_pandas_type(pandera_dtype: pandera.dtypes.DataType):
        return pandera_dtype.type.type

    def _get_col_dtypes(self, t: Type[pandera.typing.DataFrame]):
        schema, _ = self._pandera_schema(t)
        return {k: self._get_pandas_type(v.dtype) for k, v in schema.columns.items()}

    def _get_schema_type(self, t: Type[pandera.typing.DataFrame]) -> SchemaType:
        converted_cols: typing.List[SchemaType.SchemaColumn] = []
        schema, _ = self._pandera_schema(t)
        for k, col in schema.columns.items():
            pandas_type = self._get_pandas_type(col.dtype)
            if pandas_type not in self._SUPPORTED_TYPES:
                raise AssertionError(f"type {pandas_type} is currently not supported by the flytekit-pandera plugin")
            converted_cols.append(SchemaType.SchemaColumn(name=k, type=self._SUPPORTED_TYPES[pandas_type]))
        return SchemaType(columns=converted_cols)

    def get_literal_type(self, t: Type[pandera.typing.DataFrame]) -> LiteralType:
        return LiteralType(schema=self._get_schema_type(t))

    def assert_type(self, t: Type[T], v: T):
        if not hasattr(t, "__origin__") and not isinstance(v, (t, pandas.DataFrame)):
            raise TypeError(f"Type of Val '{v}' is not an instance of {t}")

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: pandas.DataFrame,
        python_type: Type[pandera.typing.DataFrame],
        expected: LiteralType,
    ) -> Literal:
        if isinstance(python_val, pandas.DataFrame):
            local_dir = ctx.file_access.get_random_local_directory()
            w = PandasSchemaWriter(
                local_dir=local_dir, cols=self._get_col_dtypes(python_type), fmt=SchemaFormat.PARQUET
            )
            schema, config = self._pandera_schema(python_type)
            try:
                validated_val = schema.validate(python_val, lazy=config.lazy)
            except (pandera.errors.SchemaError,pandera.errors.SchemaErrors) as exc:
                if config.on_error == "raise":
                    raise exc
                else:
                    renderer = PanderaReportRenderer(title="Pandera Error Report: Input")
                    Deck(renderer._title, renderer.to_html(exc))
                validated_val = python_val

            w.write(validated_val)
            remote_path = ctx.file_access.put_raw_data(local_dir)
            return Literal(scalar=Scalar(schema=Schema(remote_path, self._get_schema_type(python_type))))
        else:
            raise AssertionError(
                f"Only Pandas Dataframe object can be returned from a task, returned object type {type(python_val)}"
            )

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[pandera.typing.DataFrame]
    ) -> pandera.typing.DataFrame:
        if not (lv and lv.scalar and lv.scalar.schema):
            raise AssertionError("Can only convert a literal schema to a pandera schema")

        def downloader(x, y):
            ctx.file_access.get_data(x, y, is_multipart=True)

        sdt = literals.StructuredDatasetType(
            format=structured_dataset.StructuredDatasetTransformerEngine.DEFAULT_FORMATS.get(
                pandas.DataFrame,
                structured_dataset.GENERIC_FORMAT,
            )
        )
        sd_metadata = literals.StructuredDatasetMetadata(structured_dataset_type=sdt)
        sd = StructuredDataset(
            uri=lv.scalar.schema.uri,
            downloader=downloader,
            supported_mode=SchemaOpenMode.READ,
            metadata=sd_metadata
        )
        sd._literal_sd = literals.StructuredDataset(
            uri=lv.scalar.schema.uri,
            metadata=sd_metadata
        )

        schema, config = self._pandera_schema(expected_python_type)
        df = sd.open(pandas.DataFrame).all()
        try:
            validated_val = schema.validate(df, lazy=config.lazy)
        except (pandera.errors.SchemaError, pandera.errors.SchemaErrors) as exc:
            if config.on_error == "raise":
                raise exc
            else:
                renderer = PanderaReportRenderer(title="Pandera Error Report: Output")
                Deck(renderer._title, renderer.to_html(exc))
            validated_val = df
        return validated_val


TypeEngine.register(PanderaTransformer())
