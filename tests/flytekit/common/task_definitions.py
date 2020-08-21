from __future__ import absolute_import

from flytekit.sdk.tasks import inputs, outputs, python_task
from flytekit.sdk.types import Types


@inputs(a=Types.Integer)
@outputs(b=Types.Integer)
@python_task
def add_one(wf_params, a, b):
    b.set(a + 1)


@inputs(
    train=Types.CSV,
    validation=Types.MultiPartBlob,
    a=Types.Integer,
    b=Types.Float,
    c=Types.String,
    d=Types.Boolean,
    e=Types.Datetime,
)
@outputs(
    otrain=Types.CSV,
    ovalidation=Types.MultiPartBlob,
    oa=Types.Integer,
    ob=Types.Float,
    oc=Types.String,
    od=Types.Boolean,
    oe=Types.Datetime,
)
@python_task
def dummy_for_entrypoint_alt(wf_params, train, validation, a, b, c, d, e, otrain, ovalidation, oa, ob, oc, od, oe):
    otrain.set(train)
    ovalidation.set(validation)
    oa.set(a)
    ob.set(b)
    oc.set(c)
    od.set(d)
    oe.set(e)
