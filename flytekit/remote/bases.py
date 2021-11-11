import abc


class HasIO(abc.ABC):
    @property
    def inputs(self):
        """
        Returns the inputs to the execution in the standard Python format as dictated by the type engine.
        :rtype:  dict[Text, T]
        """
        return self._inputs

    @property
    def outputs(self):
        """
        Returns the outputs to the execution in the standard Python format as dictated by the type engine.  If the
        execution ended in error or the execution is in progress, an exception will be raised.
        :rtype:  dict[Text, T]
        """
        pass
