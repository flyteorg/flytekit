from __future__ import absolute_import
from __future__ import print_function

import six as _six
from flytekit.sdk.tasks import qubole_hive_task, outputs, inputs, python_task
from flytekit.sdk.workflow import workflow_class
from flytekit.sdk.types import Types


@outputs(hive_results=[Types.Schema()])
@qubole_hive_task(tags=[_six.text_type('these'), _six.text_type('are'), _six.text_type('tags')])
def generate_queries(wf_params, hive_results):
    q1 = "SELECT 1"
    q2 = "SELECT 'two'"
    schema_1, formatted_query_1 = Types.Schema().create_from_hive_query(select_query=q1)
    schema_2, formatted_query_2 = Types.Schema().create_from_hive_query(select_query=q2)

    hive_results.set([schema_1, schema_2])
    return [formatted_query_1, formatted_query_2]


@inputs(ss=[Types.Schema()])
@python_task
def print_schemas(wf_params, ss):
    for s in ss:
        with s as r:
            for df in r.iter_chunks():
                df = r.read()
                print(df)


@workflow_class
class ExampleQueryWorkflow(object):
    a = generate_queries()
    b = print_schemas(ss=a.outputs.hive_results)
