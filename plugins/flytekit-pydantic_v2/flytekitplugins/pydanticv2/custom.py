from pydantic import BaseModel, model_serializer
from flytekit.types.file import FlyteFile, FlyteFilePathTransformer
from flytekit.core.context_manager import FlyteContextManager
from typing import Dict, Type, Any

@model_serializer
def ser_flyte_file(self) -> Dict[str, Any]:
    lv = FlyteFilePathTransformer().to_literal(FlyteContextManager.current_context(), self, FlyteFile, None)
    return {"path": lv.scalar.blob.uri, "does serialization works?": "Yes it is!"}

setattr(FlyteFile, "ser_flyte_file", ser_flyte_file)

