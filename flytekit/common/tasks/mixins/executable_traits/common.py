from __future__ import absolute_import


class ExecutionParameters(object):

    """
    This is the parameter object that will be provided as the first parameter for every execution of any @*_task
    decorated function.
    """

    def __init__(self, execution_date, tmp_dir, stats, execution_id, logging):
        self._stats = stats
        self._execution_date = execution_date
        self._working_directory = tmp_dir
        self._execution_id = execution_id
        self._logging = logging

    @property
    def stats(self):
        """
        A handle to a special statsd object that provides usefully tagged stats.

        TODO: Usage examples and better comments

        :rtype: flytekit.interfaces.stats.taggable.TaggableStats
        """
        return self._stats

    @property
    def logging(self):
        """
        A handle to a useful logging object.

        TODO: Usage examples

        :rtype: logging
        """
        return self._logging

    @property
    def working_directory(self):
        """
        A handle to a special working directory for easily producing temporary files.

        TODO: Usage examples

        :rtype: flytekit.common.utils.AutoDeletingTempDir
        """
        return self._working_directory

    @property
    def execution_date(self):
        """
        This is a datetime representing the time at which a workflow was started.  This is consistent across all tasks
        executed in a workflow or sub-workflow.

        .. note::

            Do NOT use this execution_date to drive any production logic.  It might be useful as a tag for data to help
            in debugging.

        :rtype: datetime.datetime
        """
        return self._execution_date

    @property
    def execution_id(self):
        """
        This is the identifier of the workflow execution within the underlying engine.  It will be consistent across all
        task executions in a workflow or sub-workflow execution.

        .. note::

            Do NOT use this execution_id to drive any production logic.  This execution ID should only be used as a tag
            on output data to link back to the workflow run that created it.

        :rtype: Text
        """
        return self._execution_id
