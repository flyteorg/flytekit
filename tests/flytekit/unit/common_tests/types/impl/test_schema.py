from __future__ import absolute_import

import collections as _collections
import datetime as _datetime
import os as _os
import uuid as _uuid

import pandas as _pd
import pytest as _pytest
import six.moves as _six_moves

from flytekit.common import utils as _utils
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import blobs as _blobs
from flytekit.common.types import primitives as _primitives
from flytekit.common.types.impl import schema as _schema_impl
from flytekit.models import literals as _literal_models
from flytekit.models import types as _type_models
from flytekit.sdk import test_utils as _test_utils


def test_schema_type():
    _schema_impl.SchemaType()
    _schema_impl.SchemaType([])
    _schema_impl.SchemaType(
        [
            ("a", _primitives.Integer),
            ("b", _primitives.String),
            ("c", _primitives.Float),
            ("d", _primitives.Boolean),
            ("e", _primitives.Datetime),
        ]
    )

    with _pytest.raises(ValueError):
        _schema_impl.SchemaType({"a": _primitives.Integer})

    with _pytest.raises(TypeError):
        _schema_impl.SchemaType([("a", _blobs.Blob)])

    with _pytest.raises(ValueError):
        _schema_impl.SchemaType([("a", _primitives.Integer, 1)])

        _schema_impl.SchemaType([("1", _primitives.Integer)])
    with _pytest.raises(TypeError):
        _schema_impl.SchemaType([(1, _primitives.Integer)])

    with _pytest.raises(TypeError):
        _schema_impl.SchemaType([("1", [_primitives.Integer])])


value_type_tuples = [
    ("abra", _primitives.Integer, [1, 2, 3, 4, 5]),
    ("CADABRA", _primitives.Float, [1.0, 2.0, 3.0, 4.0, 5.0]),
    ("HoCuS", _primitives.String, ["A", "B", "C", "D", "E"]),
    ("Pocus", _primitives.Boolean, [True, False, True, False]),
    (
        "locusts",
        _primitives.Datetime,
        [
            _datetime.datetime(
                day=1, month=1, year=2017, hour=1, minute=1, second=1, microsecond=1
            )
            - _datetime.timedelta(days=i)
            for i in _six_moves.range(5)
        ],
    ),
]


@_pytest.mark.parametrize("value_type_pair", value_type_tuples)
def test_simple_read_and_write_with_different_types(value_type_pair):
    column_name, flyte_type, values = value_type_pair
    values = [tuple([value]) for value in values]
    schema_type = _schema_impl.SchemaType(columns=[(column_name, flyte_type)])

    with _test_utils.LocalTestFileSystem() as sandbox:
        with _utils.AutoDeletingTempDir("test") as t:
            a = _schema_impl.Schema.create_at_known_location(
                t.name, mode="wb", schema_type=schema_type
            )
            assert a.local_path is None
            with a as writer:
                for _ in _six_moves.range(5):
                    writer.write(
                        _pd.DataFrame.from_records(values, columns=[column_name])
                    )
                assert a.local_path.startswith(sandbox.name)
            assert a.local_path is None

            b = _schema_impl.Schema.create_at_known_location(
                t.name, mode="rb", schema_type=schema_type
            )
            assert b.local_path is None
            with b as reader:
                for df in reader.iter_chunks():
                    for check, actual in _six_moves.zip(
                        values, df[column_name].tolist()
                    ):
                        assert check[0] == actual
                assert reader.read() is None
                reader.seek(0)
                df = reader.read(concat=True)
                for iter_count, actual in enumerate(df[column_name].tolist()):
                    assert values[iter_count % len(values)][0] == actual
                assert b.local_path.startswith(sandbox.name)
            assert b.local_path is None


def test_datetime_coercion_explicitly():
    """
    Sanity check that we're using a version of pyarrow that allows us to
    truncate timestamps
    """
    dt = _datetime.datetime(
        day=1, month=1, year=2017, hour=1, minute=1, second=1, microsecond=1
    )
    values = [(dt,)]
    df = _pd.DataFrame.from_records(values, columns=["testname"])
    assert df["testname"][0] == dt

    with _utils.AutoDeletingTempDir("test") as tmpdir:
        tmpfile = tmpdir.get_named_tempfile("repro.parquet")
        df.to_parquet(tmpfile, coerce_timestamps="ms", allow_truncated_timestamps=True)
        df2 = _pd.read_parquet(tmpfile)

    dt2 = _datetime.datetime(day=1, month=1, year=2017, hour=1, minute=1, second=1)
    assert df2["testname"][0] == dt2


def test_datetime_coercion():
    values = [
        tuple(
            [
                _datetime.datetime(
                    day=1, month=1, year=2017, hour=1, minute=1, second=1, microsecond=1
                )
                - _datetime.timedelta(days=x)
            ]
        )
        for x in _six_moves.range(5)
    ]
    schema_type = _schema_impl.SchemaType(columns=[("testname", _primitives.Datetime)])

    with _test_utils.LocalTestFileSystem():
        with _utils.AutoDeletingTempDir("test") as t:
            a = _schema_impl.Schema.create_at_known_location(
                t.name, mode="wb", schema_type=schema_type
            )
            with a as writer:
                for _ in _six_moves.range(5):
                    # us to ms coercion segfaults unless we explicitly allow truncation.
                    writer.write(
                        _pd.DataFrame.from_records(values, columns=["testname"]),
                        coerce_timestamps="ms",
                        allow_truncated_timestamps=True,
                    )

                    # TODO: Uncomment when segfault bug is resolved
                    # with _pytest.raises(Exception):
                    #    writer.write(
                    #        _pd.DataFrame.from_records(values, columns=['testname']),
                    #        coerce_timestamps='ms')

            b = _schema_impl.Schema.create_at_known_location(
                t.name, mode="wb", schema_type=schema_type
            )
            with b as writer:
                for _ in _six_moves.range(5):
                    writer.write(
                        _pd.DataFrame.from_records(values, columns=["testname"])
                    )


@_pytest.mark.parametrize("value_type_pair", value_type_tuples)
def test_fetch(value_type_pair):
    column_name, flyte_type, values = value_type_pair
    values = [tuple([value]) for value in values]
    schema_type = _schema_impl.SchemaType(columns=[(column_name, flyte_type)])

    with _utils.AutoDeletingTempDir("test") as tmpdir:
        for i in _six_moves.range(3):
            _pd.DataFrame.from_records(values, columns=[column_name]).to_parquet(
                tmpdir.get_named_tempfile(str(i).zfill(6)), coerce_timestamps="us"
            )

        with _utils.AutoDeletingTempDir("test2") as local_dir:
            schema_obj = _schema_impl.Schema.fetch(
                tmpdir.name,
                local_path=local_dir.get_named_tempfile("schema_test"),
                schema_type=schema_type,
            )
            with schema_obj as reader:
                for df in reader.iter_chunks():
                    for check, actual in _six_moves.zip(
                        values, df[column_name].tolist()
                    ):
                        assert check[0] == actual
                assert reader.read() is None
                reader.seek(0)
                df = reader.read(concat=True)
                for iter_count, actual in enumerate(df[column_name].tolist()):
                    assert values[iter_count % len(values)][0] == actual


@_pytest.mark.parametrize("value_type_pair", value_type_tuples)
def test_download(value_type_pair):
    column_name, flyte_type, values = value_type_pair
    values = [tuple([value]) for value in values]
    schema_type = _schema_impl.SchemaType(columns=[(column_name, flyte_type)])

    with _utils.AutoDeletingTempDir("test") as tmpdir:
        for i in _six_moves.range(3):
            _pd.DataFrame.from_records(values, columns=[column_name]).to_parquet(
                tmpdir.get_named_tempfile(str(i).zfill(6)), coerce_timestamps="us"
            )

        with _utils.AutoDeletingTempDir("test2") as local_dir:
            schema_obj = _schema_impl.Schema(tmpdir.name, schema_type=schema_type)
            schema_obj.download(local_dir.get_named_tempfile(_uuid.uuid4().hex))
            with schema_obj as reader:
                for df in reader.iter_chunks():
                    for check, actual in _six_moves.zip(
                        values, df[column_name].tolist()
                    ):
                        assert check[0] == actual
                assert reader.read() is None
                reader.seek(0)
                df = reader.read(concat=True)
                for iter_count, actual in enumerate(df[column_name].tolist()):
                    assert values[iter_count % len(values)][0] == actual

        with _pytest.raises(Exception):
            schema_obj = _schema_impl.Schema(tmpdir.name, schema_type=schema_type)
            schema_obj.download()

        with _test_utils.LocalTestFileSystem():
            schema_obj = _schema_impl.Schema(tmpdir.name, schema_type=schema_type)
            schema_obj.download()
            with schema_obj as reader:
                for df in reader.iter_chunks():
                    for check, actual in _six_moves.zip(
                        values, df[column_name].tolist()
                    ):
                        assert check[0] == actual
                assert reader.read() is None
                reader.seek(0)
                df = reader.read(concat=True)
                for iter_count, actual in enumerate(df[column_name].tolist()):
                    assert values[iter_count % len(values)][0] == actual


def test_hive_queries(monkeypatch):
    def return_deterministic_uuid():
        class FakeUUID4(object):
            def __init__(self):
                self.hex = "test_uuid"

        class Uuid(object):
            def uuid4(self):
                return FakeUUID4()

        return Uuid()

    monkeypatch.setattr(_schema_impl, "_uuid", return_deterministic_uuid())

    all_types = _schema_impl.SchemaType(
        [
            ("a", _primitives.Integer),
            ("b", _primitives.String),
            ("c", _primitives.Float),
            ("d", _primitives.Boolean),
            ("e", _primitives.Datetime),
        ]
    )

    with _test_utils.LocalTestFileSystem():
        df, query = _schema_impl.Schema.create_from_hive_query(
            "SELECT a, b, c, d, e FROM some_place WHERE i = 0",
            stage_query="CREATE TEMPORARY TABLE some_place AS SELECT * FROM some_place_original",
            known_location="s3://my_fixed_path/",
            schema_type=all_types,
        )

        full_query = """
        CREATE TEMPORARY TABLE some_place AS SELECT * FROM some_place_original;
        CREATE TEMPORARY TABLE test_uuid_tmp AS SELECT a, b, c, d, e FROM some_place WHERE i = 0;
        CREATE EXTERNAL TABLE test_uuid LIKE test_uuid_tmp STORED AS PARQUET;
        ALTER TABLE test_uuid SET LOCATION 's3://my_fixed_path/';
        INSERT OVERWRITE TABLE test_uuid
            SELECT
                a as a,
            b as b,
            CAST(c as double) c,
            d as d,
            e as e
            FROM test_uuid_tmp;
        DROP TABLE test_uuid;
        """
        full_query = " ".join(full_query.split())
        query = " ".join(query.split())
        assert query == full_query

        # Test adding partition
        full_query = """
        ALTER TABLE some_table ADD IF NOT EXISTS PARTITION (
            region = 'SEA',
            ds = '2017-01-01'
        ) LOCATION 's3://my_fixed_path/';
        ALTER TABLE some_table PARTITION (
            region = 'SEA',
            ds = '2017-01-01'
        ) SET LOCATION 's3://my_fixed_path/';
        """
        query = df.get_write_partition_to_hive_table_query(
            "some_table",
            partitions=_collections.OrderedDict(
                [("region", "SEA"), ("ds", "2017-01-01")]
            ),
        )
        full_query = " ".join(full_query.split())
        query = " ".join(query.split())
        assert query == full_query


def test_partial_column_read():
    with _test_utils.LocalTestFileSystem():
        a = _schema_impl.Schema.create_at_any_location(
            schema_type=_schema_impl.SchemaType(
                [("a", _primitives.Integer), ("b", _primitives.Integer)]
            )
        )
        with a as writer:
            writer.write(
                _pd.DataFrame.from_dict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
            )

        b = _schema_impl.Schema.fetch(
            a.uri,
            schema_type=_schema_impl.SchemaType(
                [("a", _primitives.Integer), ("b", _primitives.Integer)]
            ),
        )
        with b as reader:
            df = reader.read(columns=["b"])
            assert df.columns.values == ["b"]
            assert df["b"].tolist() == [5, 6, 7, 8]


def test_casting():
    pass


def test_from_python_std():
    with _test_utils.LocalTestFileSystem():

        def single_dataframe():
            df1 = _pd.DataFrame.from_dict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
            s = _schema_impl.Schema.from_python_std(
                t_value=df1,
                schema_type=_schema_impl.SchemaType(
                    [("a", _primitives.Integer), ("b", _primitives.Integer)]
                ),
            )
            assert s is not None
            n = _schema_impl.Schema.fetch(
                s.uri,
                schema_type=_schema_impl.SchemaType(
                    [("a", _primitives.Integer), ("b", _primitives.Integer)]
                ),
            )
            with n as reader:
                df2 = reader.read()
                assert df2.columns.values.all() == df1.columns.values.all()
                assert df2["b"].tolist() == df1["b"].tolist()

        def list_of_dataframes():
            df1 = _pd.DataFrame.from_dict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
            df2 = _pd.DataFrame.from_dict({"a": [9, 10, 11, 12], "b": [13, 14, 15, 16]})
            s = _schema_impl.Schema.from_python_std(
                t_value=[df1, df2],
                schema_type=_schema_impl.SchemaType(
                    [("a", _primitives.Integer), ("b", _primitives.Integer)]
                ),
            )
            assert s is not None
            n = _schema_impl.Schema.fetch(
                s.uri,
                schema_type=_schema_impl.SchemaType(
                    [("a", _primitives.Integer), ("b", _primitives.Integer)]
                ),
            )
            with n as reader:
                actual = []
                for df in reader.iter_chunks():
                    assert df.columns.values.all() == df1.columns.values.all()
                    actual.extend(df["b"].tolist())
                b_val = df1["b"].tolist()
                b_val.extend(df2["b"].tolist())
                assert actual == b_val

        def mixed_list():
            df1 = _pd.DataFrame.from_dict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
            df2 = [1, 2, 3]
            with _pytest.raises(_user_exceptions.FlyteTypeException):
                _schema_impl.Schema.from_python_std(
                    t_value=[df1, df2],
                    schema_type=_schema_impl.SchemaType(
                        [("a", _primitives.Integer), ("b", _primitives.Integer)]
                    ),
                )

        def empty_list():
            s = _schema_impl.Schema.from_python_std(
                t_value=[],
                schema_type=_schema_impl.SchemaType(
                    [("a", _primitives.Integer), ("b", _primitives.Integer)]
                ),
            )
            assert s is not None
            n = _schema_impl.Schema.fetch(
                s.uri,
                schema_type=_schema_impl.SchemaType(
                    [("a", _primitives.Integer), ("b", _primitives.Integer)]
                ),
            )
            with n as reader:
                df = reader.read()
                assert df is None

        single_dataframe()
        mixed_list()
        empty_list()
        list_of_dataframes()


def test_promote_from_model_schema_type():
    m = _type_models.SchemaType(
        [
            _type_models.SchemaType.SchemaColumn(
                "a", _type_models.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN
            ),
            _type_models.SchemaType.SchemaColumn(
                "b", _type_models.SchemaType.SchemaColumn.SchemaColumnType.DATETIME
            ),
            _type_models.SchemaType.SchemaColumn(
                "c", _type_models.SchemaType.SchemaColumn.SchemaColumnType.DURATION
            ),
            _type_models.SchemaType.SchemaColumn(
                "d", _type_models.SchemaType.SchemaColumn.SchemaColumnType.FLOAT
            ),
            _type_models.SchemaType.SchemaColumn(
                "e", _type_models.SchemaType.SchemaColumn.SchemaColumnType.INTEGER
            ),
            _type_models.SchemaType.SchemaColumn(
                "f", _type_models.SchemaType.SchemaColumn.SchemaColumnType.STRING
            ),
        ]
    )
    s = _schema_impl.SchemaType.promote_from_model(m)
    assert s.columns == m.columns
    assert (
        s.sdk_columns["a"].to_flyte_literal_type()
        == _primitives.Boolean.to_flyte_literal_type()
    )
    assert (
        s.sdk_columns["b"].to_flyte_literal_type()
        == _primitives.Datetime.to_flyte_literal_type()
    )
    assert (
        s.sdk_columns["c"].to_flyte_literal_type()
        == _primitives.Timedelta.to_flyte_literal_type()
    )
    assert (
        s.sdk_columns["d"].to_flyte_literal_type()
        == _primitives.Float.to_flyte_literal_type()
    )
    assert (
        s.sdk_columns["e"].to_flyte_literal_type()
        == _primitives.Integer.to_flyte_literal_type()
    )
    assert (
        s.sdk_columns["f"].to_flyte_literal_type()
        == _primitives.String.to_flyte_literal_type()
    )
    assert s == m


def test_promote_from_model_schema():
    m = _literal_models.Schema(
        "s3://some/place/",
        _type_models.SchemaType(
            [
                _type_models.SchemaType.SchemaColumn(
                    "a", _type_models.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN
                ),
                _type_models.SchemaType.SchemaColumn(
                    "b", _type_models.SchemaType.SchemaColumn.SchemaColumnType.DATETIME
                ),
                _type_models.SchemaType.SchemaColumn(
                    "c", _type_models.SchemaType.SchemaColumn.SchemaColumnType.DURATION
                ),
                _type_models.SchemaType.SchemaColumn(
                    "d", _type_models.SchemaType.SchemaColumn.SchemaColumnType.FLOAT
                ),
                _type_models.SchemaType.SchemaColumn(
                    "e", _type_models.SchemaType.SchemaColumn.SchemaColumnType.INTEGER
                ),
                _type_models.SchemaType.SchemaColumn(
                    "f", _type_models.SchemaType.SchemaColumn.SchemaColumnType.STRING
                ),
            ]
        ),
    )

    s = _schema_impl.Schema.promote_from_model(m)
    assert s.uri == "s3://some/place/"
    assert (
        s.type.sdk_columns["a"].to_flyte_literal_type()
        == _primitives.Boolean.to_flyte_literal_type()
    )
    assert (
        s.type.sdk_columns["b"].to_flyte_literal_type()
        == _primitives.Datetime.to_flyte_literal_type()
    )
    assert (
        s.type.sdk_columns["c"].to_flyte_literal_type()
        == _primitives.Timedelta.to_flyte_literal_type()
    )
    assert (
        s.type.sdk_columns["d"].to_flyte_literal_type()
        == _primitives.Float.to_flyte_literal_type()
    )
    assert (
        s.type.sdk_columns["e"].to_flyte_literal_type()
        == _primitives.Integer.to_flyte_literal_type()
    )
    assert (
        s.type.sdk_columns["f"].to_flyte_literal_type()
        == _primitives.String.to_flyte_literal_type()
    )
    assert s == m


def test_create_at_known_location():
    with _test_utils.LocalTestFileSystem():
        with _utils.AutoDeletingTempDir("test") as wd:
            b = _schema_impl.Schema.create_at_known_location(
                wd.name, schema_type=_schema_impl.SchemaType()
            )
            assert b.local_path is None
            assert b.remote_location == wd.name + "/"
            assert b.mode == "wb"

            with b as w:
                w.write(_pd.DataFrame.from_dict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]}))

            df = _pd.read_parquet(_os.path.join(wd.name, "000000"))
            assert list(df["a"]) == [1, 2, 3, 4]
            assert list(df["b"]) == [5, 6, 7, 8]


def test_generic_schema_read():
    with _test_utils.LocalTestFileSystem():
        a = _schema_impl.Schema.create_at_any_location(
            schema_type=_schema_impl.SchemaType(
                [("a", _primitives.Integer), ("b", _primitives.Integer)]
            )
        )
        with a as writer:
            writer.write(
                _pd.DataFrame.from_dict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
            )

        b = _schema_impl.Schema.fetch(
            a.remote_prefix, schema_type=_schema_impl.SchemaType([])
        )
        with b as reader:
            df = reader.read()
            assert df.columns.values.tolist() == ["a", "b"]
            assert df["a"].tolist() == [1, 2, 3, 4]
            assert df["b"].tolist() == [5, 6, 7, 8]


def test_extra_schema_read():
    with _test_utils.LocalTestFileSystem():
        a = _schema_impl.Schema.create_at_any_location(
            schema_type=_schema_impl.SchemaType(
                [("a", _primitives.Integer), ("b", _primitives.Integer)]
            )
        )
        with a as writer:
            writer.write(
                _pd.DataFrame.from_dict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
            )

        b = _schema_impl.Schema.fetch(
            a.remote_prefix,
            schema_type=_schema_impl.SchemaType([("a", _primitives.Integer)]),
        )
        with b as reader:
            df = reader.read(concat=True, truncate_extra_columns=False)
            assert df.columns.values.tolist() == ["a", "b"]
            assert df["a"].tolist() == [1, 2, 3, 4]
            assert df["b"].tolist() == [5, 6, 7, 8]

        with b as reader:
            df = reader.read(concat=True)
            assert df.columns.values.tolist() == ["a"]
            assert df["a"].tolist() == [1, 2, 3, 4]


def test_normal_schema_read_with_fastparquet():
    with _test_utils.LocalTestFileSystem():
        a = _schema_impl.Schema.create_at_any_location(
            schema_type=_schema_impl.SchemaType(
                [("a", _primitives.Integer), ("b", _primitives.Boolean)]
            )
        )
        with a as writer:
            writer.write(
                _pd.DataFrame.from_dict(
                    {"a": [1, 2, 3, 4], "b": [False, True, True, False]}
                )
            )

        import os as _os

        original_engine = _os.getenv("PARQUET_ENGINE")
        _os.environ["PARQUET_ENGINE"] = "fastparquet"

        b = _schema_impl.Schema.fetch(
            a.remote_prefix, schema_type=_schema_impl.SchemaType([])
        )

        with b as reader:
            df = reader.read()
            assert df["a"].tolist() == [1, 2, 3, 4]
            assert _pd.api.types.is_bool_dtype(df.dtypes["b"])
            assert df["b"].tolist() == [False, True, True, False]

        if original_engine is None:
            del _os.environ["PARQUET_ENGINE"]
        else:
            _os.environ["PARQUET_ENGINE"] = original_engine


def test_schema_read_consistency_between_two_engines():
    with _test_utils.LocalTestFileSystem():
        a = _schema_impl.Schema.create_at_any_location(
            schema_type=_schema_impl.SchemaType(
                [("a", _primitives.Integer), ("b", _primitives.Boolean)]
            )
        )
        with a as writer:
            writer.write(
                _pd.DataFrame.from_dict(
                    {"a": [1, 2, 3, 4], "b": [True, True, True, False]}
                )
            )

        import os as _os

        original_engine = _os.getenv("PARQUET_ENGINE")
        _os.environ["PARQUET_ENGINE"] = "fastparquet"

        b = _schema_impl.Schema.fetch(
            a.remote_prefix, schema_type=_schema_impl.SchemaType([])
        )

        with b as b_reader:
            b_df = b_reader.read()
            _os.environ["PARQUET_ENGINE"] = "pyarrow"

            c = _schema_impl.Schema.fetch(
                a.remote_prefix, schema_type=_schema_impl.SchemaType([])
            )
            with c as c_reader:
                c_df = c_reader.read()
                assert b_df.equals(c_df)

        if original_engine is None:
            del _os.environ["PARQUET_ENGINE"]
        else:
            _os.environ["PARQUET_ENGINE"] = original_engine
