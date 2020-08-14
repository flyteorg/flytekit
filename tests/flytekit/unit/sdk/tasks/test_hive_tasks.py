from __future__ import absolute_import, print_function

import logging as _logging
from datetime import datetime as _datetime

import six as _six

from flytekit.common import utils as _common_utils
from flytekit.common.tasks import hive_task as _hive_task
from flytekit.common.tasks import output as _task_output
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.types import base_sdk_types as _base_sdk_types
from flytekit.common.types import containers as _containers
from flytekit.common.types import helpers as _type_helpers
from flytekit.common.types import schema as _schema
from flytekit.common.types.impl.schema import Schema
from flytekit.engines import common as _common_engine
from flytekit.models import literals as _literals
from flytekit.models.core.identifier import WorkflowExecutionIdentifier
from flytekit.sdk.tasks import hive_task, inputs, outputs, qubole_hive_task
from flytekit.sdk.types import Types


@hive_task(cache_version="1")
def sample_hive_task_no_input(wf_params):
    return _six.text_type("select 5")


@inputs(in1=Types.Integer)
@hive_task(cache_version="1")
def sample_hive_task(wf_params, in1):
    return _six.text_type("select ") + _six.text_type(in1)


@hive_task
def sample_hive_task_no_queries(wf_params):
    return []


@qubole_hive_task(
    cache_version="1", cluster_label=_six.text_type("cluster_label"), tags=[],
)
def sample_qubole_hive_task_no_input(wf_params):
    return _six.text_type("select 5")


@inputs(in1=Types.Integer)
@qubole_hive_task(
    cache_version="1",
    cluster_label=_six.text_type("cluster_label"),
    tags=[_six.text_type("tag1")],
)
def sample_qubole_hive_task(wf_params, in1):
    return _six.text_type("select ") + _six.text_type(in1)


def test_hive_task():
    assert isinstance(sample_hive_task, _sdk_runnable.SdkRunnableTask)
    assert isinstance(sample_hive_task, _hive_task.SdkHiveTask)

    sample_hive_task.unit_test(in1=5)


@outputs(hive_results=[Types.Schema()])
@qubole_hive_task
def two_queries(wf_params, hive_results):
    q1 = "SELECT 1"
    q2 = "SELECT 'two'"
    schema_1, formatted_query_1 = Schema.create_from_hive_query(select_query=q1)
    schema_2, formatted_query_2 = Schema.create_from_hive_query(select_query=q2)

    hive_results.set([schema_1, schema_2])
    return [formatted_query_1, formatted_query_2]


def test_interface_setup():
    outs = two_queries.interface.outputs
    assert outs["hive_results"].type.collection_type is not None
    assert outs["hive_results"].type.collection_type.schema is not None
    assert outs["hive_results"].type.collection_type.schema.columns == []


def test_sdk_output_references_construction():
    references = {
        name: _task_output.OutputReference(
            _type_helpers.get_sdk_type_from_literal_type(variable.type)
        )
        for name, variable in _six.iteritems(two_queries.interface.outputs)
    }
    # Before user code is run, the outputs passed to the user code should not have values
    assert references["hive_results"].sdk_value == _base_sdk_types.Void()

    # Should be a list of schemas
    assert isinstance(
        references["hive_results"].sdk_type, _containers.TypedCollectionType
    )
    assert isinstance(
        references["hive_results"].sdk_type.sub_type, _schema.SchemaInstantiator
    )


def test_hive_task_query_generation():
    with _common_utils.AutoDeletingTempDir("user_dir") as user_working_directory:
        context = _common_engine.EngineContext(
            execution_id=WorkflowExecutionIdentifier(
                project="unit_test", domain="unit_test", name="unit_test"
            ),
            execution_date=_datetime.utcnow(),
            stats=None,  # TODO: A mock stats object that we can read later.
            logging=_logging,  # TODO: A mock logging object that we can read later.
            tmp_dir=user_working_directory,
        )
        references = {
            name: _task_output.OutputReference(
                _type_helpers.get_sdk_type_from_literal_type(variable.type)
            )
            for name, variable in _six.iteritems(two_queries.interface.outputs)
        }

        qubole_hive_jobs = two_queries._generate_plugin_objects(context, references)
        assert len(qubole_hive_jobs) == 2

        # deprecated, collection is only here for backwards compatibility
        assert len(qubole_hive_jobs[0].query_collection.queries) == 1
        assert len(qubole_hive_jobs[1].query_collection.queries) == 1

        # The output references should now have the same fake S3 path as the formatted queries
        assert references["hive_results"].value[0].uri != ""
        assert references["hive_results"].value[1].uri != ""
        assert (
            references["hive_results"].value[0].uri in qubole_hive_jobs[0].query.query
        )
        assert (
            references["hive_results"].value[1].uri in qubole_hive_jobs[1].query.query
        )


def test_hive_task_dynamic_job_spec_generation():
    with _common_utils.AutoDeletingTempDir("user_dir") as user_working_directory:
        context = _common_engine.EngineContext(
            execution_id=WorkflowExecutionIdentifier(
                project="unit_test", domain="unit_test", name="unit_test"
            ),
            execution_date=_datetime.utcnow(),
            stats=None,  # TODO: A mock stats object that we can read later.
            logging=_logging,  # TODO: A mock logging object that we can read later.
            tmp_dir=user_working_directory,
        )
        dj_spec = two_queries._produce_dynamic_job_spec(
            context, _literals.LiteralMap(literals={})
        )

        # Bindings
        assert len(dj_spec.outputs[0].binding.collection.bindings) == 2
        assert isinstance(
            dj_spec.outputs[0].binding.collection.bindings[0].scalar.schema, Schema
        )
        assert isinstance(
            dj_spec.outputs[0].binding.collection.bindings[1].scalar.schema, Schema
        )

        # Custom field is filled in
        assert len(dj_spec.tasks[0].custom) > 0
