from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekit.types.schema import FlyteSchema

from .custom import (
    deserialize_flyte_dir,
    deserialize_flyte_file,
    deserialize_flyte_schema,
    serialize_flyte_dir,
    serialize_flyte_file,
    serialize_flyte_schema,
)
from .transformer import PydanticTransformer

setattr(FlyteFile, "serialize_flyte_file", serialize_flyte_file)
setattr(FlyteFile, "deserialize_flyte_file", deserialize_flyte_file)
setattr(FlyteDirectory, "serialize_flyte_dir", serialize_flyte_dir)
setattr(FlyteDirectory, "deserialize_flyte_dir", deserialize_flyte_dir)
setattr(FlyteSchema, "serialize_flyte_schema", serialize_flyte_schema)
setattr(FlyteSchema, "deserialize_flyte_schema", deserialize_flyte_schema)
