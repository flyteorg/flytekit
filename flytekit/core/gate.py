from __future__ import annotations

import datetime
import typing
from typing import Tuple, Union

from flytekit.core import interface as flyte_interface
from flytekit.core.context_manager import FlyteContext
from flytekit.core.promise import (
    Promise,
    VoidPromise,
    flyte_entity_call_handler,
)
from flytekit.core.type_engine import TypeEngine
from flytekit.models.core import workflow as _workflow_model
from flytekit.models.types import LiteralType

DEFAULT_TIMEOUT = datetime.timedelta(hours=1)


class Gate(object):
    """
    A gate needs to behave kind of like a task. Instead of running code however, it either needs to wait for user
    input, or run a timer.
    """

    def __init__(self, name: str, input_type: typing.Optional[typing.Type] = None,
                 sleep_duration: typing.Optional[datetime.timedelta] = None,
                 timeout: typing.Optional[datetime.timedelta] = None):
        self._name = name
        self._input_type = input_type
        self._sleep_duration = sleep_duration
        self._timeout = timeout or DEFAULT_TIMEOUT

        self._literal_type = TypeEngine.to_literal_type(input_type) if input_type else None

    @property
    def name(self) -> str:
        # Part of SupportsNodeCreation interface
        return self._name

    @property
    def input_type(self) -> typing.Optional[typing.Type]:
        return self._input_type

    @property
    def literal_type(self) -> typing.Optional[LiteralType]:
        return self._literal_type

    @property
    def sleep_duration(self) -> typing.Optional[datetime.timedelta]:
        return self._sleep_duration

    @property
    def python_interface(self) -> flyte_interface.Interface:
        # Part of SupportsNodeCreation interface
        # If this is just a sleep node, then nothing happens
        if self.sleep_duration:
            return flyte_interface.Interface()

        # If this is a signaling node then the output should be of the type requested by the signal.
        return flyte_interface.Interface(outputs={
            "o0": self.input_type,
        })

    def construct_node_metadata(self) -> _workflow_model.NodeMetadata:
        # Part of SupportsNodeCreation interface
        return _workflow_model.NodeMetadata(
            name=self.name,
            timeout=self._timeout,
        )

    # This is to satisfy the LocallyExecutable protocol
    def local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, VoidPromise]:
        if self.sleep_duration:
            print(f"Fake sleeping {self.name}")
            return VoidPromise(self.name)

        # Triggering stdin is easy, can use that to ask for input, but doing that in tests is hard.
        # TODO: FIGURE OUT HOW TO DO THIS!
        if issubclass(self.input_type, bool):
            l = TypeEngine.to_literal(ctx, True, bool, TypeEngine.to_literal_type(bool))  # noqa
            p = Promise(var="o0", val=l)
            return p
        elif issubclass(self.input_type, int):
            l = TypeEngine.to_literal(ctx, 42, int, TypeEngine.to_literal_type(int))  # noqa
            p = Promise(var="o0", val=l)
            return p
        else:
            raise Exception("only bool or int")


def signal(name: str, timeout: datetime.timedelta, expected_type: typing.Type):
    # Create a Gate object. This object will function like a task. Note that unlike a task, each time this function
    # is called, a new Python object is created. If a workflow calls a subworkflow twice, and the subworkflow has
    # a signal, then two Gate objects are created. This shouldn't be a problem as long as the objects are identical.

    g = Gate(name, input_type=expected_type, timeout=timeout)

    return flyte_entity_call_handler(g)
