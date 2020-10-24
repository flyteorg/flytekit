from flytekit.common.types import blobs as _blobs
from flytekit.common.types import containers as _containers
from flytekit.common.types import helpers as _helpers
from flytekit.common.types import primitives as _primitives
from flytekit.common.types import proto as _proto
from flytekit.common.types import schema as _schema


class Types(object):
    Integer = _helpers.get_sdk_type_from_literal_type(_primitives.Integer.to_flyte_literal_type())
    """
    Use this to specify a simple integer type.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            1) If set, A Python int will be received, if set.
            2) Otherwise, a None value will be received.

        As output:
            1) User code may pass an int or long value.
            2) Output can also be nulled with a None value.

        From command-line:
            Specify an integer or integer string.

    .. code-block:: python

        @inputs(a=Types.Integer)
        @outputs(b=Types.Integer)
        @python_task
        def double(wf_params, a, b):
            b.set(a * 2)
    """

    Float = _helpers.get_sdk_type_from_literal_type(_primitives.Float.to_flyte_literal_type())
    """
    Use this to specify a simple floating point type.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            A Python float will be received, if set.  Otherwise, a None value will be received.

        As output:
            User code may pass a float value.  It can also be nulled with a None value.

        From command-line:
            Specify a float or floating-point string.

    .. code-block:: python

        @inputs(a=Types.Float)
        @outputs(b=Types.Float)
        @python_task
        def square(wf_params, a, b):
            b.set(a * a)
    """

    String = _helpers.get_sdk_type_from_literal_type(_primitives.String.to_flyte_literal_type())
    """
    Use this to specify a simple string type.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            A Python str (Python 2) or unicode (Python 3) will be received, if set.  Otherwise, a None value will be
            received.

        As output:
            User code may pass a str value (Python 2) or a unicode value (Python 3).  It can also be nulled with a None
            value.

        From command-line:
            Specify a string.

    .. code-block:: python

        @inputs(a=Types.String, b=Types.String)
        @outputs(c=Types.String)
        @python_task
        def concat(wf_params, a, b):
            c.set(a + b)
    """

    Boolean = _helpers.get_sdk_type_from_literal_type(_primitives.Boolean.to_flyte_literal_type())
    """
    Use this to specify a simple bool type.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            A Python bool will be received, if set.  Otherwise, a None value will be received.

        As output:
            User code may pass a bool value.  It can also be nulled with a None value.

        From command-line:
            Specify 0, 1, true, or false.

    .. code-block:: python

        @inputs(a=Types.Boolean)
        @outputs(b=Types.Boolean)
        @python_task
        def invert(wf_params, a, b):
            b.set(not a)
    """

    Datetime = _helpers.get_sdk_type_from_literal_type(_primitives.Datetime.to_flyte_literal_type())
    """
    Use this to specify a simple datetime type.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            A Python timezone-aware datetime.datetime will be received with a UTC time, if set.  Otherwise,
            a None value will be received.

        As output:
            User code may pass a timezone-aware datetime.datetime value.  It can also be nulled with a None value.

        From command-line:
            Specify a timezone-aware, parsable datestring.  i.e. 2019-01-01T00:00+00:00

    .. note::

        The engine requires that datetimes be timezone aware.  By default, Python datetime.datetime is not timezone
        aware.

    .. code-block:: python

        @inputs(a=Types.Datetime)
        @outputs(b=Types.Datetime)
        @python_task
        def tomorrow(wf_params, a, b):
            b.set(a + datetime.timedelta(days=1))
    """

    Timedelta = _helpers.get_sdk_type_from_literal_type(_primitives.Timedelta.to_flyte_literal_type())
    """
    Use this to specify a simple timedelta type.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            A Python datetime.timedelta will be received, if set.  Otherwise, a None value will be received.

        As output:
            User code may pass a datetime.timedelta value.  It can also be nulled with a None value.

        From command-line:
            Specify a parsable duration string.  i.e. 1h30m24s

    .. code-block:: python

        @inputs(a=Types.Timedelta)
        @outputs(b=Types.Timedelta)
        @python_task
        def hundred_times_longer(wf_params, a, b):
            b.set(a * 100)
    """

    Generic = _helpers.get_sdk_type_from_literal_type(_primitives.Generic.to_flyte_literal_type())
    """
    Use this to specify a simple JSON type. The Generic type offer a flexible (but loose) extension to flyte's typing
    system by allowing custom types/objects to be passed through. It's strongly recommended for producers & consumers of
    entities that produce or consume a Generic type to perform their own expectations checks on the integrity of the
    object.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            1) If set, a Python dict with JSON-ifiable primitives and nested lists or maps.
            2) Otherwise, a None value will be received.

        As output:
            1) User code may pass a Python dict with arbitrarily nested lists and dictionaries.  JSON-ifiable
               primitives may also be specified.
            2) Output can also be nulled with a None value.

        From command-line:
            Specify a JSON string.

    .. code-block:: python

        @inputs(a=Types.Generic)
        @outputs(b=Types.Generic)
        @python_task
        def operate(wf_params, a, b):
            if a['operation'] == 'add':
                a['value'] += a['operand']  # a['value'] is a number
            elif a['operation'] == 'merge':
                a['value'].update(a['some']['nested'][0]['field'])
            b.set(a)

        # For better readability, it's strongly advised to leverage python's type aliasing.
        MyTypeA = Types.Generic
        MyTypeB = Types.Generic

        # This makes it clearer that it received a certain type and produces a different one. Other tasks that consume
        # MyTypeB should do so in their input declaration.
        @inputs(a=MyTypeA)
        @outputs(b=MyTypeB)
        @python_task
        def operate(wf_params, a, b):
            if a['operation'] == 'add':
                a['value'] += a['operand']  # a['value'] is a number
            elif a['operation'] == 'merge':
                a['value'].update(a['some']['nested'][0]['field'])
            b.set(a)
    """

    Blob = _blobs.Blob
    """
    Use this to specify a Blob object which is essentially a managed file.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            1) If set, a :py:class:`flytekit.common.types.impl.blobs.Blob` object will be received.
            2) If not set, a None value.

        As output:
            1) A user may specify a path string.
            2) A user may construct a :py:class:`flytekit.common.types.impl.blobs.Blob` object and pass it as output.
            3) Output can be nulled with a None value.

        From command-line:
            Specify a path to the blob. This path must be accessible from the container when executing--either by
            being downloaded from an accessible remote location like s3 or as a local file.

    .. code-block:: python

        @inputs(a=Types.Blob)
        @outputs(b=Types.Blob)
        @python_task
        def copy(wf_params, a, b):
            with a as reader:
                txt = reader.read()

            out = Types.Blob()  # Create at a random location specified in flytekit configuration
            with out as writer:
                writer.write(txt)
            b.set(out)
    """

    CSV = _blobs.CSV
    """
    Use this to specify a CSV blob object which is essentially a managed file in the CSV format.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            1) If set, a :py:class:`flytekit.common.types.impl.blobs.CSV` object will be received.
            2) If not set, a None value.

        As output:
            1) A user may specify a path string.
            2) A user may construct a :py:class:`flytekit.common.types.impl.blobs.CSV` object and pass it as output.
            3) Output can be nulled with a None value.

        From command-line:
            Specify a path to the CSV. This path must be accessible from the container when executing--either by
            being downloaded from an accessible remote location like s3 or as a local file.

    .. code-block:: python

        @inputs(a=Types.CSV)
        @outputs(b=Types.CSV)
        @python_task
        def copy(wf_params, a, b):
            with a as reader:
                txt = reader.read()

            out = Types.CSV()  # Create at a random location specified in flytekit configuration
            with out as writer:
                writer.write(txt)
            b.set(out)
    """

    MultiPartBlob = _blobs.MultiPartBlob
    """
    Use this to specify a multi-part blob object which is essentially a chunked file in a non-recursive directory.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            1) If set, a :py:class:`flytekit.common.types.impl.blobs.MultiPartBlob` object will be received.
            2) If not set, a None value.

        As output:
            1) A user may specify a path string.
            2) A user may construct a :py:class:`flytekit.common.types.impl.blobs.MultiPartBlob` object and pass it as
               output.
            3) Output can be nulled with a None value.

        From command-line:
            Specify a path to the multi-part blob. This path must be accessible from the container when
            executing--either by being downloaded from an accessible remote location like s3 or as a local file.

    .. code-block:: python

        @inputs(a=Types.MultiPartBlob)
        @outputs(b=Types.MultiPartBlob)
        @python_task
        def concat_then_split(wf_params, a, b):
            txt = ""
            with a as chunks:
                for chunk in chunks:
                    txt += chunk.read()

            out = Types.MultiPartBlob()  # Create at a random location specified in flytekit configuration
            with out.create_part('000000') as writer:
                writer.write("Chunk1")
            with out.create_part('000001') as writer:
                writer.write("Chunk2")
            b.set(out)
    """

    MultiPartCSV = _blobs.MultiPartCSV
    """
    See :py:attr:`flytekit.sdk.types.Types.MultiPartBlob`, but in CSV format
    """

    Schema = staticmethod(_schema.schema_instantiator)
    """
    Use this to specify a Schema blob object which is essentially a chunked stream of Parquet dataframes.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        Cast behavior:
            1) A generic schema (specified as `Types.Schema()`) can receive input from any schema type regardless of
               column definitions.
            2) A schema can receive as input any schema object as long as the upstream schema has a superset of the
               column names defined and the types match for paired columns.  Ordering does not matter.

        As input:
            1) If set, a :py:class:`flytekit.common.types.impl.schema.Schema` object will be received.
            2) If not set, a None value.

        As output:
            1) A user may specify a path string to a chunked dataframe non-recursive directory.
            2) A user may construct a :py:class:`flytekit.common.types.impl.schema.Schema` object (with the correct
               column definitions) and pass it as output.
            3) Output can be nulled with a None value.

        From command-line:
            Specify a path to the schema object. This path must be accessible from the container when
            executing--either by being downloaded from an accessible remote location like s3 or as a local file.

    .. code-block:: python

        @inputs(generic=Types.Schema(), typed=Types.Schema([('a', Types.Integer), ('b', Types.Float)]))
        @outputs(b=Types.Schema([('a', Types.Integer), ('b', Types.Float)]))
        @python_task
        def concat_then_split(wf_params, generic, typed,):
            with typed as reader:
                # Each chunk is loaded as a pandas.DataFrame object
                for df in reader.iter_chunks():
                    # Operate on the dataframe

            # Create at a random location specified in flytekit configuration
            out = Types.Schema([('a', Types.Integer), ('b', Types.Float)])()
            with out as writer:
                writer.write(
                    pandas.DataFrame.from_dict(
                        {
                            'a': [1, 2, 3],
                            'b': [5.0, 6.0, 7.0]
                        }
                    )
                )
            b.set(out)
    """

    Proto = staticmethod(_proto.create_protobuf)
    """
    Proto type wraps a protobuf type to provide interoperability between protobuf and flyte typing system. Using this
    type, you can define custom input/output variable types of flyte entities and continue to provide strong typing
    syntax. Proto type serializes proto objects as binary (leveraging `flyteidl's Binary literal <https://github.com/lyft/flyteidl/blob/793b09d190148236f41ad8160b5cec9a3325c16f/protos/flyteidl/core/literals.proto#L45>`_).
    Binary serialization of protobufs is the most space-efficient serialization form. Because of the way protobufs are
    designed, unmarshalling the serialized proto requires access to the corresponding type. In order to use/visualize
    the serialized proto, you will generally need to write custom code in the corresponding component.

    .. note::

        The protobuf Python library should be installed on the PYTHONPATH to ensure the type engine can access the
        appropriate Python code to deserialize the protobuf.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            1) If set, a Python protobuf object of the type specified in the definition.
            2) If not set, a None value.

        As output:
            1) A Python protobuf object matching the type specified by the users.
            2) Set None to null the output.

        From command-line:
            A base-64 encoded string of the serialized protobuf.

    .. code-block:: python

        from protos import my_protos_pb2

        @inputs(a=Types.Proto(my_protos_pb2.Custom))
        @outputs(b=Types.Proto(my_protos_pb2.Custom))
        @python_task
        def assert_and_create(wf_params, a, b):
            assert a.field1 == 1
            assert a.field2 == 'abc'
            b.set(
                my_protos_pb2.Custom(
                    field1=100,
                    field2='hello'
                )
            )
    """

    GenericProto = staticmethod(_proto.create_generic)
    """
    GenericProto type wraps a protobuf type to provide interoperability between protobuf and flyte typing system. Using
    this type, you can define custom input/output variable types of flyte entities and continue to provide strong typing
    syntax. Proto type serializes proto objects as binary (leveraging `flyteidl's Binary literal <https://github.com/lyft/flyteidl/blob/793b09d190148236f41ad8160b5cec9a3325c16f/protos/flyteidl/core/literals.proto#L63>`_).
    A generic proto is a specialization of the Generic type with added convenience functions to support marshalling/
    unmarshalling of the underlying protobuf object using the protobuf official json marshaller. While GenericProto type
    does not produce the most space-efficient representation of protobufs, it's a suitable solution for making protobufs
    easily accessible (i.e. humanly readable) in other flyte components (e.g. console, cli... etc.).

    .. note::

        The protobuf Python library should be installed on the PYTHONPATH to ensure the type engine can access the
        appropriate Python code to deserialize the protobuf.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            1) If set, a Python protobuf object of the type specified in the definition.
            2) If not set, a None value.

        As output:
            1) A Python protobuf object matching the type specified by the users.
            2) Set None to null the output.

        From command-line:
            A base-64 encoded string of the serialized protobuf.

    .. code-block:: python

        from protos import my_protos_pb2

        @inputs(a=Types.GenericProto(my_protos_pb2.Custom))
        @outputs(b=Types.GenericProto(my_protos_pb2.Custom))
        @python_task
        def assert_and_create(wf_params, a, b):
            assert a.field1 == 1
            assert a.field2 == 'abc'
            b.set(
                my_protos_pb2.Custom(
                    field1=100,
                    field2='hello'
                )
            )
    """

    List = staticmethod(_containers.List)
    """
    Use this to specify a list of any type--including nested lists.

    When used with an SDK-decorated method, expect this behavior from the default type engine:

        As input:
            1) If set, a Python list populated with values matching the behavior of the list's sub-type.
            2) If not set, a None value.

        As output:
            1) A Python list containing values adhering to the list's sub-type.
            2) Set None to null the output.

        From command-line:
            Specify a valid JSON list string.  The sub-values will be checked against the sub-type of the list.

    .. note::

        Shorthand syntax is supported of the form: `[Types.Integer]` in addition to longhand syntax like
        `Types.List(Types.Integer)`.  Both forms are equivalent.

    .. note::

        Lists can be arbitrarily deeply nested, however, the typing must be consistent between all sibling values in a
        nested list.  Syntax for nesting is `[[[Types.Integer]]]` to create a 3D list of integers.

    .. code-block:: python

        @inputs(a=[Types.Integer])
        @outputs(b=[Types.Integer])
        @python_task
        def square_each(wf_params, a, b):
            b.set(
                [x * x for x in a]
            )
    """
