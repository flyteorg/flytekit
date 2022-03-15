import textwrap
import typing

from flytekit.models import interface as _interface_models
from flytekit.models.interface import Variable


class TypedInterface(_interface_models.TypedInterface):
    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.interface.TypedInterface model:
        :rtype: TypedInterface
        """
        return cls(model.inputs, model.outputs)

    @staticmethod
    def map_printer(m: typing.Dict[str, Variable]) -> str:
        r = []
        for k, v in m.items():
            r.append(f"{k}: {v.type}")
        return "\n".join(r)

    def verbose_string(self, indent: int = 0) -> str:
        inputs = ""
        if self.inputs:
            t = f"""\
            Inputs:
            {textwrap.indent(self.map_printer(self.inputs), "  ")}
            """
            inputs = textwrap.dedent(t)

        outputs = ""
        if self.outputs:
            t = f"""\
            Outputs:
            {textwrap.indent(self.map_printer(self.outputs), "  ")}
            """
            outputs = textwrap.dedent(t)

        return inputs + outputs
