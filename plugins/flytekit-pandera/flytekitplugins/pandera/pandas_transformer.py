import typing
from typing import TYPE_CHECKING, Type

from flytekit import Deck, FlyteContext, lazy_module
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.models.literals import Literal
from flytekit.models.types import LiteralType, SchemaType
from flytekit.types.structured import structured_dataset

from .config import PanderaValidationConfig
from .pandas_renderer import PandasReportRenderer

if TYPE_CHECKING:
    import pandas

    import pandera
else:
    pandas = lazy_module("pandas")
    pandera = lazy_module("pandera")


T = typing.TypeVar("T")


class PanderaPandasTransformer(TypeTransformer[pandera.typing.DataFrame]):
    _SUPPORTED_TYPES: typing.Dict[type, SchemaType.SchemaColumn.SchemaColumnType] = (
        structured_dataset.get_supported_types()
    )

    def __init__(self):
        super().__init__("Pandera Transformer", pandera.typing.DataFrame)  # type: ignore
        self._sd_transformer = structured_dataset.StructuredDatasetTransformerEngine()

    def _get_pandera_schema(self, t: Type[pandera.typing.DataFrame]):
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
        schema, _ = self._get_pandera_schema(t)
        return {k: self._get_pandas_type(v.dtype) for k, v in schema.columns.items()}

    def get_literal_type(self, t: Type[pandera.typing.DataFrame]) -> LiteralType:
        if typing.get_origin(t) is typing.Annotated:
            t, _ = typing.get_args(t)
        return self._sd_transformer.get_literal_type(t)

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
        assert isinstance(python_val, pandas.DataFrame), \
            f"Only Pandas Dataframe object can be returned from a task, returned object type {type(python_val)}"

        schema, config = self._get_pandera_schema(python_type)
        renderer = PandasReportRenderer(title="Pandera Report: Output")
        try:
            val = schema.validate(python_val, lazy=True)
        except (pandera.errors.SchemaError, pandera.errors.SchemaErrors) as exc:
            if config.on_error == "raise":
                raise exc
            elif config.on_error == "warn":
                html = renderer.to_html(exc)
                Deck(renderer._title, html)
            else:
                raise ValueError(f"Invalid on_error value: {config.on_error}")
            val = python_val

        return self._sd_transformer.to_literal(ctx, val, pandas.DataFrame, expected)

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[pandera.typing.DataFrame]
    ) -> pandera.typing.DataFrame:
        if not (lv and lv.scalar and lv.scalar.structured_dataset):
            raise AssertionError("Can only convert a literal structured dataset to a pandera schema")

        df = self._sd_transformer.to_python_value(ctx, lv, pandas.DataFrame)
        schema, config = self._get_pandera_schema(expected_python_type)
        renderer = PandasReportRenderer(title="Pandera Report: Input")
        try:
            val = schema.validate(df, lazy=True)
        except (pandera.errors.SchemaError, pandera.errors.SchemaErrors) as exc:
            if config.on_error == "raise":
                raise exc
            elif config.on_error == "warn":
                html = renderer.to_html(exc)
                Deck(renderer._title, html)
            else:
                raise ValueError(f"Invalid on_error value: {config.on_error}")
            val = df

        return val


TypeEngine.register(PanderaPandasTransformer())
