import tempfile
import typing
from dataclasses import dataclass
from typing import Type

import dolt_integrations.core as dolt_int
import doltcli as dolt
import pandas
from dataclasses_json import dataclass_json
from google.protobuf.struct_pb2 import Struct

from flytekit import FlyteContext
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.models import types as _type_models
from flytekit.models.literals import Literal, Scalar
from flytekit.models.types import LiteralType


@dataclass_json
@dataclass
class DoltConfig:
    db_path: str
    tablename: typing.Optional[str] = None
    sql: typing.Optional[str] = None
    io_args: typing.Optional[dict] = None
    branch_conf: typing.Optional[dolt_int.Branch] = None
    meta_conf: typing.Optional[dolt_int.Meta] = None
    remote_conf: typing.Optional[dolt_int.Remote] = None


@dataclass_json
@dataclass
class DoltTable:
    config: DoltConfig
    data: typing.Optional[pandas.DataFrame] = None


class DoltTableNameTransformer(TypeTransformer[DoltTable]):
    def __init__(self):
        super().__init__(name="DoltTable", t=DoltTable)

    def get_literal_type(self, t: Type[DoltTable]) -> LiteralType:
        return LiteralType(simple=_type_models.SimpleType.STRUCT, metadata={})

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: DoltTable,
        python_type: typing.Type[DoltTable],
        expected: LiteralType,
    ) -> Literal:

        if not isinstance(python_val, DoltTable):
            raise AssertionError(f"Value cannot be converted to a table: {python_val}")

        conf = python_val.config
        if python_val.data is not None and python_val.config.tablename is not None:
            db = dolt.Dolt(conf.db_path)
            with tempfile.NamedTemporaryFile() as f:
                python_val.data.to_csv(f.name, index=False)
                dolt_int.save(
                    db=db,
                    tablename=conf.tablename,
                    filename=f.name,
                    branch_conf=conf.branch_conf,
                    meta_conf=conf.meta_conf,
                    remote_conf=conf.remote_conf,
                    save_args=conf.io_args,
                )

        s = Struct()
        s.update(python_val.to_dict())
        return Literal(Scalar(generic=s))

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: typing.Type[DoltTable],
    ) -> DoltTable:

        if not (lv and lv.scalar and lv.scalar.generic and lv.scalar.generic["config"]):
            return pandas.DataFrame()

        conf = DoltConfig(**lv.scalar.generic["config"])
        db = dolt.Dolt(conf.db_path)

        with tempfile.NamedTemporaryFile() as f:
            dolt_int.load(
                db=db,
                tablename=conf.tablename,
                sql=conf.sql,
                filename=f.name,
                branch_conf=conf.branch_conf,
                meta_conf=conf.meta_conf,
                remote_conf=conf.remote_conf,
                load_args=conf.io_args,
            )
            df = pandas.read_csv(f)
            lv.data = df

        return lv


TypeEngine.register(DoltTableNameTransformer())
