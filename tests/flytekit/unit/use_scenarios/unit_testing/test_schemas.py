from __future__ import absolute_import

from flytekit.sdk.types import Types
from flytekit.sdk.tasks import inputs, outputs, python_task
from flytekit.sdk.test_utils import flyte_test
from flytekit.common.exceptions import user as _user_exceptions
import pandas as pd
import pytest


@flyte_test
def test_generic_schema():
    @inputs(a=Types.Schema())
    @outputs(b=Types.Schema())
    @python_task
    def copy_task(wf_params, a, b):
        out = Types.Schema()()
        with a as r:
            with out as w:
                for df in r.iter_chunks():
                    w.write(df)
        b.set(out)

    # Test generic copy and pass through
    a = Types.Schema()()
    with a as w:
        w.write(
            pd.DataFrame.from_dict(
                {
                    'a': [1, 2, 3],
                    'b': [4.0, 5.0, 6.0]
                }
            )
        )
        w.write(
            pd.DataFrame.from_dict(
                {
                    'a': [3, 2, 1],
                    'b': [6.0, 5.0, 4.0]
                }
            )
        )

    outs = copy_task.unit_test(a=a)

    with outs['b'] as r:
        df = r.read()
        assert list(df['a']) == [1, 2, 3]
        assert list(df['b']) == [4.0, 5.0, 6.0]

        df = r.read()
        assert list(df['a']) == [3, 2, 1]
        assert list(df['b']) == [6.0, 5.0, 4.0]

        assert r.read() is None

    # Test typed copy and pass through
    a = Types.Schema([('a', Types.Integer), ('b', Types.Float)])()
    with a as w:
        w.write(
            pd.DataFrame.from_dict(
                {
                    'a': [1, 2, 3],
                    'b': [4.0, 5.0, 6.0]
                }
            )
        )
        w.write(
            pd.DataFrame.from_dict(
                {
                    'a': [3, 2, 1],
                    'b': [6.0, 5.0, 4.0]
                }
            )
        )

    outs = copy_task.unit_test(a=a)

    with outs['b'] as r:
        df = r.read()
        assert list(df['a']) == [1, 2, 3]
        assert list(df['b']) == [4.0, 5.0, 6.0]

        df = r.read()
        assert list(df['a']) == [3, 2, 1]
        assert list(df['b']) == [6.0, 5.0, 4.0]

        assert r.read() is None


@flyte_test
def test_typed_schema():
    @inputs(a=Types.Schema([('a', Types.Integer), ('b', Types.Float)]))
    @outputs(b=Types.Schema([('a', Types.Integer), ('b', Types.Float)]))
    @python_task
    def copy_task(wf_params, a, b):
        out = Types.Schema([('a', Types.Integer), ('b', Types.Float)])()
        with a as r:
            with out as w:
                for df in r.iter_chunks():
                    w.write(df)
        b.set(out)

    # Test typed copy and pass through
    a = Types.Schema([('a', Types.Integer), ('b', Types.Float)])()
    with a as w:
        w.write(
            pd.DataFrame.from_dict(
                {
                    'a': [1, 2, 3],
                    'b': [4.0, 5.0, 6.0]
                }
            )
        )
        w.write(
            pd.DataFrame.from_dict(
                {
                    'a': [3, 2, 1],
                    'b': [6.0, 5.0, 4.0]
                }
            )
        )

    outs = copy_task.unit_test(a=a)

    with outs['b'] as r:
        df = r.read()
        assert list(df['a']) == [1, 2, 3]
        assert list(df['b']) == [4.0, 5.0, 6.0]

        df = r.read()
        assert list(df['a']) == [3, 2, 1]
        assert list(df['b']) == [6.0, 5.0, 4.0]

        assert r.read() is None

    # Test untyped failure
    a = Types.Schema()()
    with a as w:
        w.write(
            pd.DataFrame.from_dict(
                {
                    'a': [1, 2, 3],
                    'b': [4.0, 5.0, 6.0]
                }
            )
        )
        w.write(
            pd.DataFrame.from_dict(
                {
                    'a': [3, 2, 1],
                    'b': [6.0, 5.0, 4.0]
                }
            )
        )

    with pytest.raises(_user_exceptions.FlyteTypeException):
        copy_task.unit_test(a=a)


@flyte_test
def test_subset_of_columns():
    @outputs(a=Types.Schema([('a', Types.Integer), ('b', Types.String)]))
    @python_task()
    def source(wf_params, a):
        out = Types.Schema([('a', Types.Integer), ('b', Types.String)])()
        with out as writer:
            writer.write(
                pd.DataFrame.from_dict(
                    {
                        'a': [1, 2, 3, 4, 5],
                        'b': ['a', 'b', 'c', 'd', 'e']
                    }
                )
            )
        a.set(out)

    @inputs(a=Types.Schema([('a', Types.Integer)]))
    @python_task()
    def sink(wf_params, a):
        with a as reader:
            df = reader.read(concat=True)
            assert len(df.columns.values) == 1
            assert df['a'].tolist() == [1, 2, 3, 4, 5]

        with a as reader:
            df = reader.read(truncate_extra_columns=False)
            assert df.columns.values.tolist() == ['a', 'b']
            assert df['a'].tolist() == [1, 2, 3, 4, 5]
            assert df['b'].tolist() == ['a', 'b', 'c', 'd', 'e']

    o = source.unit_test()
    sink.unit_test(**o)


@flyte_test
def test_no_output_set():
    @outputs(a=Types.Schema())
    @python_task()
    def null_set(wf_params, a):
        pass

    assert null_set.unit_test()['a'] is None
