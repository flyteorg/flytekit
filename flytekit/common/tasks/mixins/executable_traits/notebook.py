from __future__ import absolute_import
import abc as _abc
import json as _json
import os as _os
import six as _six

from flyteidl.core import literals_pb2 as _literals_pb2, interface_pb2 as _interface_pb2
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.common import constants as _constants, interface as _interface, utils as _utils
from flytekit.common.tasks.mixins.executable_traits import common as _common
from flytekit.common.types import helpers as _type_helpers
from flytekit.models import literals as _literals, interface as _interface
from flytekit.plugins import papermill as _papermill
from flytekit.sdk import types as _types

_HIDDEN_INPUT_CELL_FORMAT = """
# !! SYSTEM GENERATED. IGNORE. DO NOT COPY OR CHECK-IN !!
from flytekit.common.tasks.mixins.executable_traits.notebook import inject_inputs as __flyte_inject_inputs

__flyte_inputs_dict = __flyte_inject_inputs({variable_map_bytes}, {input_bytes}, {working_directory})
"""

_SHOWN_INPUT_CELL_FORMAT = "{input} = __flyte_inputs_dict.get('{input}')\n"

_HIDDEN_OUTPUT_CELL_FORMAT = """
# !! SYSTEM GENERATED. IGNORE. DO NOT COPY OR CHECK-IN !!
from flytekit.common.tasks.mixins.executable_traits.notebook import handle_outputs as __flyte_handle_outputs

__flyte_handle_outputs({variable_map_bytes}, {python_std_map}, {scratch_directory})
"""


def inject_inputs(variable_map_bytes, input_bytes, working_directory):
    """
    This method forwards necessary context into the notebook Kernel. Ideally, this code shouldn't be duplicating what
    is in the underlying engine, but for now...
    :param bytes variable_map_bytes:
    :param bytes input_bytes:
    :param Text working_directory:
    :rtype: dict[Text,Any]
    """
    if not _os.path.exists(working_directory):
        tmpdir = _utils.AutoDeletingTempDir("nb_made_")
        tmpdir.__enter__()
        working_directory = tmpdir.name
    _data_proxy.LocalWorkingDirectoryContext(working_directory).__enter__()
    _data_proxy.RemoteDataContext()

    lm_pb2 = _literals_pb2.LiteralMap()
    lm_pb2.ParseFromString(input_bytes)

    vm_pb2 = _interface_pb2.VariableMap()
    vm_pb2.ParseFromString(variable_map_bytes)

    # TODO: Inject vargs and wf_params
    return _type_helpers.unpack_literal_map_to_sdk_python_std(
        _literals.LiteralMap.from_flyte_idl(lm_pb2),
        {
            k: _type_helpers.get_sdk_type_from_literal_type(v.type)
            for k, v in _six.iteritems(_interface.VariableMap.from_flyte_idl(vm_pb2).variables)
        }
    )


def handle_user_returned(task_module, task_name, last_cell_output, scratch_directory):
    """
    Currently, there is a short-coming here where we need to load the task object to understand the correct way to
    handle the user returned value. This means the task object must be stored as a module-level attribute. This is
    not ideal and should be fixed because it makes writing tests annoying.
    :param Text task_module:
    :param Text task_name:
    :param Any last_cell_output:
    :param Text scratch_directory:
    """
    pass


def handle_outputs(variable_map_bytes, python_std_map, scratch_directory):
    """
    This function absorbs values from the notebook and stores them as an output literal map.
    :param bytes variable_map_bytes:
    :param dict[Text,Any] python_std_map:
    :param Text scratch_directory:
    """
    vm_pb2 = _interface_pb2.VariableMap()
    vm_pb2.ParseFromString(variable_map_bytes)
    outputs = _interface.VariableMap.from_flyte_idl(vm_pb2)
    literal_map = _type_helpers.pack_python_std_map_to_literal_map(
        python_std_map,
        {
            k: _type_helpers.get_sdk_type_from_literal_type(v.type)
            for k, v in _six.iteritems(outputs.variables) if k in python_std_map
        }
    )

    if _os.path.isdir(scratch_directory):
        _utils.write_proto_to_file(
            literal_map.to_flyte_idl(),
            _os.path.join(scratch_directory, _constants.OUTPUT_FILE_NAME)
        )


class NotebookTask(_six.with_metaclass(_abc.ABCMeta, _common.ExecutableTaskMixin)):

    DEFAULT_INPUT_CELL_NAME = "parameters"
    INJECTED_TAG = "flyte_injected"
    AUGMENTED_SUFFIX = ".aug.ipynb"
    OUTPUT_SUFFIX = ".out.ipynb"
    OUTPUT_NOTEBOOK = 'output_notebook'

    def __init__(self, notebook_path=None, inputs=None, outputs=None, **kwargs):
        super(NotebookTask, self).__init__(**kwargs)
        self._notebook_path = _os.path.normpath(
            _os.path.join(
                _os.path.dirname(self.instantiated_in_file),
                notebook_path
            )
        )

        # TODO: Pull interface from notebook if not provided
        inputs = inputs or dict()
        outputs = outputs or dict()
        # Add output_notebook as an implicit output to the task.
        if type(self).OUTPUT_NOTEBOOK in outputs:
            raise ValueError(
                "{} is a reserved output. Please use a different output name.".format(
                    type(self).OUTPUT_NOTEBOOK
                )
            )

        outputs[type(self).OUTPUT_NOTEBOOK] = _interface.Variable(
            _types.Types.Blob.to_flyte_literal_type(),
            "This is a copy of the resulting, completed notebook."
        )
        self.add_inputs(inputs)
        self.add_outputs(outputs)

    def _augment_notebook(self, context, inputs):
        pb2_inputs = _type_helpers.pack_python_std_map_to_literal_map(
            inputs,
            self.interface.inputs
        ).to_flyte_idl()
        with open(self._notebook_path) as json_file:
            data = _json.load(json_file)
            insert_before = 0
            for idx, p in enumerate(data['cells']):
                if type(self).DEFAULT_INPUT_CELL_NAME in p.get('metadata', {}).get('tags', []):
                    insert_before = idx + 1
                    break

            # Insert a cell that assigns Flyte inputs by their name.
            # TODO: Include var args!
            data['cells'].insert(
                insert_before,
                {
                    'source': [
                        _SHOWN_INPUT_CELL_FORMAT.format(input=input_key)
                        for input_key in self.interface.inputs.keys()
                    ],
                    'cell_type': 'code',
                    'metadata': {'tags': [type(self).INJECTED_TAG]},
                    'outputs': [],
                    'execution_count': 0,
                }
            )
            # Insert a cell which loads all the Flyte inputs using the SDK engine.
            data['cells'].insert(
                0,
                {
                    'metadata': {
                        'tags':  [
                            'hide_input',
                            'hide_output',
                            type(self).INJECTED_TAG,
                        ],
                    },
                    'outputs': [],
                    'cell_type': 'code',
                    'execution_count': 0,
                    'source': _HIDDEN_INPUT_CELL_FORMAT.format(
                        variable_map_bytes=repr(self.interface.to_flyte_idl().inputs.SerializeToString()),
                        input_bytes=repr(pb2_inputs.SerializeToString()),
                        working_directory=repr(context.working_directory.name),
                    ).splitlines(True),
                }
            )
            # Insert a cell which handles the outputs using the SDK type engine. Do not provide the handler for the
            # output notebook since that will not be retrieved from inside the notebook.
            output_str = ",".join(
                "'{key}': locals().get('{key}')".format(key=k)
                for k in _six.iterkeys(self.interface.outputs) if k != type(self).OUTPUT_NOTEBOOK
            )
            data['cells'].append(
                {
                    'metadata': {
                        'tags':  [
                            'hide_input',
                            'hide_output',
                            type(self).INJECTED_TAG,
                        ],
                    },
                    'outputs': [],
                    'cell_type': 'code',
                    'execution_count': 0,
                    'source': _HIDDEN_OUTPUT_CELL_FORMAT.format(
                        variable_map_bytes=repr(self.interface.to_flyte_idl().outputs.SerializeToString()),
                        python_std_map="{" + output_str + "}",
                        scratch_directory=repr(context.working_directory.name),
                    ).splitlines(True),
                }
            )
        with open(self._get_augmented_notebook_path(context, type(self).AUGMENTED_SUFFIX), 'w') as writer:
            writer.write(_json.dumps(data))

    def _get_augmented_notebook_path(self, context, suffix):
        """
        TODO: Doc
        :param context:
        :param Text suffix:
        :rtype: Text
        """
        return _os.path.join(context.working_directory.name, _os.path.basename(self._notebook_path) + suffix)

    def _pack_output_references(self, context, references):
        """
        TODO: Doc
        :param context:
        :return:
        """
        # Set the implicit output to reference the IPython notebook
        references[type(self).OUTPUT_NOTEBOOK].set(self._get_augmented_notebook_path(context, type(self).OUTPUT_SUFFIX))
        # Pack up what the references are set to like any other task.
        super(NotebookTask, self)._pack_output_references(context, references)
        # If an output file was already generated, then merge those values into the existing references.
        output_path = _os.path.join(context.working_directory.name, _constants.OUTPUT_FILE_NAME)
        if _os.path.exists(output_path):
            with open(output_path, 'r') as r:
                lm_pb2 = _literals_pb2.LiteralMap()
                lm_pb2.ParseFromString(r.read())
                context.output_protos[_constants.OUTPUT_FILE_NAME].literals.update(
                    _literals.LiteralMap.from_flyte_idl(lm_pb2).literals
                )

    def _execute_user_code(self, context, vargs, inputs, outputs):
        """
        TODO: Doc
        :param context:
        :param vargs:
        :param inputs:
        :param outputs:
        :return:
        """
        self._augment_notebook(context, inputs)
        # TODO: Don't use papermill for this, it is too heavyweight.
        _papermill.execute_notebook(
            self._get_augmented_notebook_path(context, type(self).AUGMENTED_SUFFIX),
            self._get_augmented_notebook_path(context, type(self).OUTPUT_SUFFIX)
        )
