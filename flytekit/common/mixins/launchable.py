from __future__ import absolute_import

import abc as _abc

import six as _six
from deprecated import deprecated as _deprecated


class LaunchableEntity(_six.with_metaclass(_abc.ABCMeta, object)):
    def launch(
        self,
        project,
        domain,
        inputs=None,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
    ):
        """
        Creates a remote execution from the entity and returns the execution identifier.
        This version of launch is meant for when inputs are specified as Python native types/structures.

        :param Text project:
        :param Text domain:
        :param dict[Text, Any] inputs: A dictionary of Python standard inputs that will be type-checked, then compiled
            to a LiteralMap.
        :param Text name: [Optional] If specified, an execution will be created with this name.  Note: the name must
            be unique within the context of the project and domain.
        :param list[flytekit.common.notifications.Notification] notification_overrides: [Optional] If specified, these
            are the notifications that will be honored for this execution.  An empty list signals to disable all
            notifications.
        :param flytekit.models.common.Labels label_overrides:
        :param flytekit.models.common.Annotations annotation_overrides:
        :rtype: T

        """
        return self.launch_with_literals(
            project,
            domain,
            self._python_std_input_map_to_literal_map(inputs or {}),
            name=name,
            notification_overrides=notification_overrides,
            label_overrides=label_overrides,
            annotation_overrides=annotation_overrides,
        )

    @_deprecated(reason="Use launch instead", version="0.9.0")
    def execute(
        self,
        project,
        domain,
        inputs=None,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
    ):
        """
        Deprecated.
        """
        return self.launch(
            project,
            domain,
            inputs=inputs,
            name=name,
            notification_overrides=notification_overrides,
            label_overrides=label_overrides,
            annotation_overrides=annotation_overrides,
        )

    @_abc.abstractmethod
    def _python_std_input_map_to_literal_map(self, inputs):
        pass

    @_abc.abstractmethod
    def launch_with_literals(
        self,
        project,
        domain,
        literal_inputs,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
    ):
        """
        Executes the entity and returns the execution identifier.  This version of execution is meant for when
        you already have a LiteralMap of inputs.

        :param Text project:
        :param Text domain:
        :param flytekit.models.literals.LiteralMap literal_inputs: Inputs to the execution.
        :param Text name: [Optional] If specified, an execution will be created with this name.  Note: the name must
            be unique within the context of the project and domain.
        :param list[flytekit.common.notifications.Notification] notification_overrides: [Optional] If specified, these
            are the notifications that will be honored for this execution.  An empty list signals to disable all
            notifications.
        :param flytekit.models.common.Labels label_overrides:
        :param flytekit.models.common.Annotations annotation_overrides:
        :rtype: flytekit.models.core.identifier.WorkflowExecutionIdentifier:
        """
        pass

    @_deprecated(reason="Use launch_with_literals instead", version="0.9.0")
    def execute_with_literals(
        self,
        project,
        domain,
        literal_inputs,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
    ):
        """
        Deprecated.
        """
        return self.launch_with_literals(
            project, domain, literal_inputs, name, notification_overrides, label_overrides, annotation_overrides,
        )
