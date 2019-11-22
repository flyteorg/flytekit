from __future__ import absolute_import

import six as _six

from flytekit.common.exceptions import scopes as _exception_scopes, user as _user_exception
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.tasks.mixins.executable_traits import notebook as _notebook_mixin
from flytekit.sdk import types as _types
from flytekit.plugins import papermill as _papermill, scrapbook as _scrapbook


OUTPUT_NOTEBOOK = 'output_notebook'


class PapermillNotebookMixin(_notebook_mixin.NotebookTask):

    # TODO: This can probably be made way more flexible with scrapbook
    _SUPPORTED_TYPES = {
        _types.Types.Integer.to_flyte_literal_type(),
        _types.Types.Boolean.to_flyte_literal_type(),
        _types.Types.Float.to_flyte_literal_type(),
        _types.Types.String.to_flyte_literal_type(),
        _types.Types.Datetime.to_flyte_literal_type(),
        _types.Types.Timedelta.to_flyte_literal_type(),
        _types.Types.Generic.to_flyte_literal_type(),
    }

    def _validate_inputs(self, inputs):
        """
        Overriding from flytekit.common.tasks.sdk_runnable.SdkRunnableTask
        """
        super(PapermillNotebookMixin, self)._validate_inputs(inputs)
        for k, v in _six.iteritems(inputs):
            # TODO: Add ability to use collections
            if v.type not in type(self)._SUPPORTED_TYPES:
                raise _user_exception.FlyteAssertion(
                    "Could not accept input '{}' because it is not a supported type. Supported types for Papermill "
                    "tasks must all be JSON serializable. ".format(k)
                )

    def _validate_outputs(self, outputs):
        """
        Overriding from flytekit.common.tasks.sdk_runnable.SdkRunnableTask
        """
        super(PapermillNotebookMixin, self)._validate_outputs(outputs)
        for k, v in _six.iteritems(outputs):
            # TODO: Add ability to use collections
            if k is not type(self).OUTPUT_NOTEBOOK and v.type not in type(self)._SUPPORTED_TYPES:
                raise _user_exception.FlyteAssertion(
                    "Could not accept output '{}' because it is not a supported type. Supported types for Papermill "
                    "tasks must all be JSON serializable. ".format(k)
                )

    def _pack_output_references(self, context, references):
        """
        Overriding from flytekit.common.tasks.mixins.executable_traits.notebook.NotebookTask
        """
        nb = _scrapbook.read_notebook(self._get_augmented_notebook_path(context, type(self).OUTPUT_SUFFIX))
        for k, ref in _six.iteritems(references):
            if k in nb.scraps:
                scrap = nb.scraps.get(k)
                if scrap.encoder != 'json':
                    raise _user_exception.FlyteValueException(
                        scrap.encoder,
                        "All scraps returned from papermill must be JSON encoded."
                    )
                ref.set(scrap.data)
        super(PapermillNotebookMixin, self)._pack_output_references(context, references)

    def _execute_user_code(self, context, vargs, inputs, outputs):
        """
        Overriding from flytekit.common.tasks.mixins.executable_traits.notebook.NotebookTask
        """
        _exception_scopes.user_entry_point(_papermill.execute_notebook)(
            self._notebook_path,
            self._get_augmented_notebook_path(context, type(self).OUTPUT_SUFFIX),
            parameters=inputs
        )


class PapermillNotebookTask(PapermillNotebookMixin, _sdk_runnable.SdkRunnableTask):
    pass
