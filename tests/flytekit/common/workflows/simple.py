from __future__ import absolute_import, print_function

from flytekit.sdk.tasks import python_task, inputs, outputs
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input
import pandas as _pd


@inputs(a=Types.Integer)
@outputs(b=Types.Integer)
@python_task
def add_one(wf_params, a, b):
    b.set(a + 1)


@inputs(a=Types.Integer)
@outputs(b=Types.Integer)
@python_task(cache=True, cache_version='1')
def subtract_one(wf_params, a, b):
    b.set(a - 1)


@outputs(
    a=Types.Blob,
    b=Types.CSV,
    c=Types.MultiPartCSV,
    d=Types.MultiPartBlob,
    e=Types.Schema(
        [
            ('a', Types.Integer),
            ('b', Types.Integer)
        ]
    )
)
@python_task
def write_special_types(wf_params, a, b, c, d, e):
    blob = Types.Blob()
    with blob as w:
        w.write("hello I'm a blob".encode('utf-8'))

    csv = Types.CSV()
    with csv as w:
        w.write("hello,i,iz,blob")

    mpcsv = Types.MultiPartCSV()
    with mpcsv.create_part('000000') as w:
        w.write("hello,i,iz,blob")
    with mpcsv.create_part('000001') as w:
        w.write("hello,i,iz,blob2")

    mpblob = Types.MultiPartBlob()
    with mpblob.create_part('000000') as w:
        w.write("hello I'm a mp blob".encode('utf-8'))
    with mpblob.create_part('000001') as w:
        w.write("hello I'm a mp blob too".encode('utf-8'))

    schema = Types.Schema([('a', Types.Integer), ('b', Types.Integer)])()
    with schema as w:
        w.write(_pd.DataFrame.from_dict({'a': [1, 2, 3], 'b': [4, 5, 6]}))
        w.write(_pd.DataFrame.from_dict({'a': [3, 2, 1], 'b': [6, 5, 4]}))

    a.set(blob)
    b.set(csv)
    c.set(mpcsv)
    d.set(mpblob)
    e.set(schema)


@inputs(
    a=Types.Blob,
    b=Types.CSV,
    c=Types.MultiPartCSV,
    d=Types.MultiPartBlob,
    e=Types.Schema(
        [
            ('a', Types.Integer),
            ('b', Types.Integer)
        ]
    )
)
@python_task
def read_special_types(wf_params, a, b, c, d, e):
    with a as r:
        assert r.read().decode('utf-8') == "hello I'm a blob"

    with b as r:
        assert r.read() == "hello,i,iz,blob"

    with c as r:
        assert len(r) == 2
        assert r[0].read() == "hello,i,iz,blob"
        assert r[1].read() == "hello,i,iz,blob2"

    with d as r:
        assert len(r) == 2
        assert r[0].read().decode('utf-8') == "hello I'm a mp blob"
        assert r[1].read().decode('utf-8') == "hello I'm a mp blob too"

    with e as r:
        df = r.read()
        assert df['a'].tolist() == [1, 2, 3]
        assert df['b'].tolist() == [4, 5, 6]

        df = r.read()
        assert df['a'].tolist() == [3, 2, 1]
        assert df['b'].tolist() == [6, 5, 4]
        assert r.read() is None


@workflow_class
class SimpleWorkflow(object):
    input_1 = Input(Types.Integer)
    input_2 = Input(Types.Integer, default=5, help='Not required.')
    a = add_one(a=input_1)
    b = add_one(a=input_2)
    c = subtract_one(a=input_1)

    d = write_special_types()
    e = read_special_types(
        a=d.outputs.a,
        b=d.outputs.b,
        c=d.outputs.c,
        d=d.outputs.d,
        e=d.outputs.e,
    )
