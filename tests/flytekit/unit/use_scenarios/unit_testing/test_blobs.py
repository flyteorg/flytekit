from __future__ import absolute_import

from flytekit.sdk.types import Types
from flytekit.sdk.tasks import inputs, outputs, python_task
from flytekit.sdk.test_utils import flyte_test
from flytekit.common.utils import AutoDeletingTempDir


@flyte_test
def test_create_blob_from_local_path():
    @outputs(a=Types.Blob)
    @python_task
    def test_create_from_local_path(wf_params, a):
        with AutoDeletingTempDir("t") as tmp:
            tmp_name = tmp.get_named_tempfile("abc.blob")
            with open(tmp_name, "wb") as w:
                w.write("Hello world".encode("utf-8"))
            a.set(tmp_name)

    out = test_create_from_local_path.unit_test()
    assert len(out) == 1
    with out["a"] as r:
        assert r.read().decode("utf-8") == "Hello world"


@flyte_test
def test_write_blob():
    @outputs(a=Types.Blob)
    @python_task
    def test_write(wf_params, a):
        b = Types.Blob()
        with b as w:
            w.write("Hello world".encode("utf-8"))
        a.set(b)

    out = test_write.unit_test()
    assert len(out) == 1
    with out["a"] as r:
        assert r.read().decode("utf-8") == "Hello world"


@flyte_test
def test_blob_passing():
    @inputs(a=Types.Blob)
    @outputs(b=Types.Blob)
    @python_task
    def test_pass(wf_params, a, b):
        b.set(a)

    b = Types.Blob()
    with b as w:
        w.write("Hello world".encode("utf-8"))

    out = test_pass.unit_test(a=b)
    assert len(out) == 1
    with out["b"] as r:
        assert r.read().decode("utf-8") == "Hello world"

    out = test_pass.unit_test(a=out["b"])
    assert len(out) == 1
    with out["b"] as r:
        assert r.read().decode("utf-8") == "Hello world"


@flyte_test
def test_create_multipartblob_from_local_path():
    @outputs(a=Types.MultiPartBlob)
    @python_task
    def test_create_from_local_path(wf_params, a):
        with AutoDeletingTempDir("t") as tmp:
            with open(tmp.get_named_tempfile("0"), "wb") as w:
                w.write("Hello world".encode("utf-8"))
            with open(tmp.get_named_tempfile("1"), "wb") as w:
                w.write("Hello world2".encode("utf-8"))
            a.set(tmp.name)

    out = test_create_from_local_path.unit_test()
    assert len(out) == 1
    with out["a"] as r:
        assert len(r) == 2
        assert r[0].read().decode("utf-8") == "Hello world"
        assert r[1].read().decode("utf-8") == "Hello world2"


@flyte_test
def test_write_multipartblob():
    @outputs(a=Types.MultiPartBlob)
    @python_task
    def test_write(wf_params, a):
        b = Types.MultiPartBlob()
        with b.create_part("0") as w:
            w.write("Hello world".encode("utf-8"))
        with b.create_part("1") as w:
            w.write("Hello world2".encode("utf-8"))
        a.set(b)

    out = test_write.unit_test()
    assert len(out) == 1
    with out["a"] as r:
        assert len(r) == 2
        assert r[0].read().decode("utf-8") == "Hello world"
        assert r[1].read().decode("utf-8") == "Hello world2"


@flyte_test
def test_multipartblob_passing():
    @inputs(a=Types.MultiPartBlob)
    @outputs(b=Types.MultiPartBlob)
    @python_task
    def test_pass(wf_params, a, b):
        b.set(a)

    b = Types.MultiPartBlob()
    with b.create_part("0") as w:
        w.write("Hello world".encode("utf-8"))
    with b.create_part("1") as w:
        w.write("Hello world2".encode("utf-8"))

    out = test_pass.unit_test(a=b)
    assert len(out) == 1
    with out["b"] as r:
        assert len(r) == 2
        assert r[0].read().decode("utf-8") == "Hello world"
        assert r[1].read().decode("utf-8") == "Hello world2"

    out = test_pass.unit_test(a=out["b"])
    assert len(out) == 1
    with out["b"] as r:
        assert len(r) == 2
        assert r[0].read().decode("utf-8") == "Hello world"
        assert r[1].read().decode("utf-8") == "Hello world2"


@flyte_test
def test_write_csv():
    @outputs(a=Types.CSV)
    @python_task
    def test_write(wf_params, a):
        b = Types.CSV()
        with b as w:
            w.write("Hello,world,hi")
        a.set(b)

    out = test_write.unit_test()
    assert len(out) == 1
    with out["a"] as r:
        assert r.read() == "Hello,world,hi"


@flyte_test
def test_write_multipartcsv():
    @outputs(a=Types.MultiPartCSV)
    @python_task
    def test_write(wf_params, a):
        b = Types.MultiPartCSV()
        with b.create_part("0") as w:
            w.write("Hello,world,1")
        with b.create_part("1") as w:
            w.write("Hello,world,2")
        a.set(b)

    out = test_write.unit_test()
    assert len(out) == 1
    with out["a"] as r:
        assert len(r) == 2
        assert r[0].read() == "Hello,world,1"
        assert r[1].read() == "Hello,world,2"
