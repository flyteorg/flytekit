import argparse
import base64
import os

from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit.core import utils
from flytekit.core.context_manager import FlyteContextManager
from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.models import literals as _literal_models
from flytekit.models.core.types import BlobType
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile


class AttrDict(dict):
    "Convert a dictionary to an attribute style lookup. Do not use this in regular places, this is used for namespacing inputs and outputs"

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def materialize_placeholder(encoded_modelfile: str) -> str:
    decoded_modelfile = base64.b64decode(encoded_modelfile).decode("utf-8")

    ctx = FlyteContextManager.current_context()
    local_inputs_file = os.path.join(ctx.execution_state.working_dir, "inputs.pb")
    ctx.file_access.get_data(
        "{{.input}}",
        local_inputs_file,
    )
    input_proto = utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)
    idl_input_literals = _literal_models.LiteralMap.from_flyte_idl(input_proto)

    inputs = literal_map_string_repr(idl_input_literals)

    for var_name, literal in idl_input_literals.literals.items():
        if literal.scalar.blob:
            if literal.scalar.blob.metadata.type.dimensionality == BlobType.BlobDimensionality.SINGLE:
                downloaded_file = FlyteFile.from_source(literal.scalar.blob.uri).download()
                inputs[var_name] = downloaded_file
            elif literal.scalar.blob.metadata.type.dimensionality == BlobType.BlobDimensionality.MULTIPART:
                downloaded_directory = FlyteDirectory.from_source(literal.scalar.blob.uri).download()
                inputs[var_name] = downloaded_directory

    inputs = {"inputs": AttrDict(inputs)}

    formatted_modelfile = decoded_modelfile.format(**inputs)
    return formatted_modelfile


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process an encoded modelfile string.")
    parser.add_argument("--encoded_modelfile", type=str, help="Base64 encoded modelfile string.")

    args = parser.parse_args()

    print(materialize_placeholder(encoded_modelfile=args.encoded_modelfile))
