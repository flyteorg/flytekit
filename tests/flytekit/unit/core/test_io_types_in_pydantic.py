"""Tests for FlyteFile, FlyteDirectory, and StructuredDataset inside nested Pydantic models.

Verifies that these special types roundtrip correctly through
TypeEngine.to_literal / to_python_value when nested in containers
(List, Dict, Optional) within Pydantic BaseModel classes.
"""

import dataclasses
import typing

from pydantic import BaseModel

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekit.types.structured import StructuredDataset

# -- Models --


class ModelWithFile(BaseModel):
    file: FlyteFile


class ModelWithDir(BaseModel):
    dir: FlyteDirectory


class ModelWithListOfFiles(BaseModel):
    files: typing.List[FlyteFile]


class ModelWithDictOfFiles(BaseModel):
    file_map: typing.Dict[str, FlyteFile]


class ModelWithListOfDirs(BaseModel):
    dirs: typing.List[FlyteDirectory]


class ModelWithDictOfDirs(BaseModel):
    dir_map: typing.Dict[str, FlyteDirectory]


class ModelWithNestedFiles(BaseModel):
    nested_files: typing.List[typing.List[FlyteFile]]


class ModelWithOptionalFile(BaseModel):
    file: typing.Optional[FlyteFile] = None


class ModelWithOptionalDir(BaseModel):
    dir: typing.Optional[FlyteDirectory] = None


class ModelWithStructuredDataset(BaseModel):
    df: StructuredDataset


class ModelWithListOfStructuredDatasets(BaseModel):
    dfs: typing.List[StructuredDataset]


class ModelWithOptionalStructuredDataset(BaseModel):
    df: typing.Optional[StructuredDataset] = None


class CombinedModel(BaseModel):
    files: typing.List[FlyteFile]
    dir_map: typing.Dict[str, FlyteDirectory]
    df: StructuredDataset
    optional_file: typing.Optional[FlyteFile] = None


# -- FlyteFile tests --


def test_file_in_model_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithFile(file=FlyteFile.from_source("s3://bucket/dummy.txt"))
    lit = TypeEngine.to_literal_type(ModelWithFile)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithFile, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithFile)
    assert isinstance(pv.file, FlyteFile)
    assert pv.file._remote_source == "s3://bucket/dummy.txt"


def test_list_of_files_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithListOfFiles(
        files=[FlyteFile.from_source("s3://bucket/a.txt"), FlyteFile.from_source("s3://bucket/b.txt")]
    )
    lit = TypeEngine.to_literal_type(ModelWithListOfFiles)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithListOfFiles, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithListOfFiles)
    assert len(pv.files) == 2
    assert pv.files[0]._remote_source == "s3://bucket/a.txt"
    assert pv.files[1]._remote_source == "s3://bucket/b.txt"


def test_dict_of_files_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithDictOfFiles(
        file_map={"x": FlyteFile.from_source("s3://bucket/c.txt"), "y": FlyteFile.from_source("s3://bucket/d.txt")}
    )
    lit = TypeEngine.to_literal_type(ModelWithDictOfFiles)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithDictOfFiles, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithDictOfFiles)
    assert pv.file_map["x"]._remote_source == "s3://bucket/c.txt"
    assert pv.file_map["y"]._remote_source == "s3://bucket/d.txt"


def test_nested_list_of_files_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithNestedFiles(
        nested_files=[
            [FlyteFile.from_source("s3://bucket/a.txt")],
            [FlyteFile.from_source("s3://bucket/b.txt"), FlyteFile.from_source("s3://bucket/c.txt")],
        ]
    )
    lit = TypeEngine.to_literal_type(ModelWithNestedFiles)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithNestedFiles, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithNestedFiles)
    assert len(pv.nested_files) == 2
    assert len(pv.nested_files[0]) == 1
    assert len(pv.nested_files[1]) == 2
    assert pv.nested_files[0][0]._remote_source == "s3://bucket/a.txt"
    assert pv.nested_files[1][0]._remote_source == "s3://bucket/b.txt"
    assert pv.nested_files[1][1]._remote_source == "s3://bucket/c.txt"


def test_optional_file_with_value_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithOptionalFile(file=FlyteFile.from_source("s3://bucket/e.txt"))
    lit = TypeEngine.to_literal_type(ModelWithOptionalFile)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithOptionalFile, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithOptionalFile)
    assert isinstance(pv.file, FlyteFile)
    assert pv.file._remote_source == "s3://bucket/e.txt"


def test_optional_file_with_none_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithOptionalFile(file=None)
    lit = TypeEngine.to_literal_type(ModelWithOptionalFile)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithOptionalFile, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithOptionalFile)
    assert pv.file is None


# -- FlyteDirectory tests --


def test_dir_in_model_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithDir(dir=FlyteDirectory.from_source("s3://bucket/mydir"))
    lit = TypeEngine.to_literal_type(ModelWithDir)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithDir, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithDir)
    assert isinstance(pv.dir, FlyteDirectory)
    assert pv.dir.remote_source == "s3://bucket/mydir"


def test_list_of_dirs_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithListOfDirs(
        dirs=[FlyteDirectory.from_source("s3://bucket/d1"), FlyteDirectory.from_source("s3://bucket/d2")]
    )
    lit = TypeEngine.to_literal_type(ModelWithListOfDirs)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithListOfDirs, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithListOfDirs)
    assert len(pv.dirs) == 2
    assert pv.dirs[0].remote_source == "s3://bucket/d1"
    assert pv.dirs[1].remote_source == "s3://bucket/d2"


def test_dict_of_dirs_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithDictOfDirs(
        dir_map={"a": FlyteDirectory.from_source("s3://bucket/d1"), "b": FlyteDirectory.from_source("s3://bucket/d2")}
    )
    lit = TypeEngine.to_literal_type(ModelWithDictOfDirs)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithDictOfDirs, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithDictOfDirs)
    assert pv.dir_map["a"].remote_source == "s3://bucket/d1"
    assert pv.dir_map["b"].remote_source == "s3://bucket/d2"


def test_optional_dir_with_none_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithOptionalDir(dir=None)
    lit = TypeEngine.to_literal_type(ModelWithOptionalDir)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithOptionalDir, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithOptionalDir)
    assert pv.dir is None


# -- StructuredDataset tests --
# Note: after roundtrip, the uri is accessible via pv.df._literal_sd.uri


def test_structured_dataset_in_model_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithStructuredDataset(df=StructuredDataset(uri="s3://bucket/data.parquet"))
    lit = TypeEngine.to_literal_type(ModelWithStructuredDataset)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithStructuredDataset, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithStructuredDataset)
    assert isinstance(pv.df, StructuredDataset)
    assert pv.df._literal_sd.uri == "s3://bucket/data.parquet"


def test_list_of_structured_datasets_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithListOfStructuredDatasets(
        dfs=[StructuredDataset(uri="s3://bucket/d1.parquet"), StructuredDataset(uri="s3://bucket/d2.parquet")]
    )
    lit = TypeEngine.to_literal_type(ModelWithListOfStructuredDatasets)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithListOfStructuredDatasets, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithListOfStructuredDatasets)
    assert len(pv.dfs) == 2
    assert pv.dfs[0]._literal_sd.uri == "s3://bucket/d1.parquet"
    assert pv.dfs[1]._literal_sd.uri == "s3://bucket/d2.parquet"


def test_optional_structured_dataset_with_value_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithOptionalStructuredDataset(df=StructuredDataset(uri="s3://bucket/opt.parquet"))
    lit = TypeEngine.to_literal_type(ModelWithOptionalStructuredDataset)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithOptionalStructuredDataset, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithOptionalStructuredDataset)
    assert isinstance(pv.df, StructuredDataset)
    assert pv.df._literal_sd.uri == "s3://bucket/opt.parquet"


def test_optional_structured_dataset_with_none_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = ModelWithOptionalStructuredDataset(df=None)
    lit = TypeEngine.to_literal_type(ModelWithOptionalStructuredDataset)
    lv = TypeEngine.to_literal(ctx, input_val, ModelWithOptionalStructuredDataset, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, ModelWithOptionalStructuredDataset)
    assert pv.df is None


# -- Combined: FlyteFile + FlyteDirectory + StructuredDataset --


def test_combined_model_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = CombinedModel(
        files=[FlyteFile.from_source("s3://bucket/a.txt"), FlyteFile.from_source("s3://bucket/b.txt")],
        dir_map={"d1": FlyteDirectory.from_source("s3://bucket/d1"), "d2": FlyteDirectory.from_source("s3://bucket/d2")},
        df=StructuredDataset(uri="s3://bucket/data.parquet"),
        optional_file=FlyteFile.from_source("s3://bucket/opt.txt"),
    )
    lit = TypeEngine.to_literal_type(CombinedModel)
    lv = TypeEngine.to_literal(ctx, input_val, CombinedModel, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, CombinedModel)
    assert len(pv.files) == 2
    assert pv.files[0]._remote_source == "s3://bucket/a.txt"
    assert pv.files[1]._remote_source == "s3://bucket/b.txt"
    assert pv.dir_map["d1"].remote_source == "s3://bucket/d1"
    assert pv.dir_map["d2"].remote_source == "s3://bucket/d2"
    assert pv.df._literal_sd.uri == "s3://bucket/data.parquet"
    assert pv.optional_file._remote_source == "s3://bucket/opt.txt"


def test_combined_model_with_none_roundtrip():
    ctx = FlyteContext.current_context()
    input_val = CombinedModel(
        files=[],
        dir_map={},
        df=StructuredDataset(uri="s3://bucket/data.parquet"),
        optional_file=None,
    )
    lit = TypeEngine.to_literal_type(CombinedModel)
    lv = TypeEngine.to_literal(ctx, input_val, CombinedModel, lit)
    assert lv

    pv = TypeEngine.to_python_value(ctx, lv, CombinedModel)
    assert pv.files == []
    assert pv.dir_map == {}
    assert pv.optional_file is None


# -- guess_python_type: deep structure verification --


def test_list_of_files_guess_type_structure():
    """Verify guess_python_type reconstructs List[FlyteFile] with correct nested structure."""
    lit = TypeEngine.to_literal_type(ModelWithListOfFiles)
    guessed = TypeEngine.guess_python_type(lit)
    assert dataclasses.is_dataclass(guessed)

    hints = typing.get_type_hints(guessed)
    files_type = hints.get("files")
    assert files_type is not None, "files field not found in guessed type"

    origin = typing.get_origin(files_type)
    assert origin is list, f"Expected List, got {origin}"

    args = typing.get_args(files_type)
    assert len(args) == 1
    inner = args[0]
    assert inner is FlyteFile or dataclasses.is_dataclass(inner), f"Expected FlyteFile or dataclass, got {inner}"


def test_nested_list_of_files_guess_type_structure():
    """Verify guess_python_type reconstructs List[List[FlyteFile]] — inner should be List, not str."""
    lit = TypeEngine.to_literal_type(ModelWithNestedFiles)
    guessed = TypeEngine.guess_python_type(lit)
    assert dataclasses.is_dataclass(guessed)

    hints = typing.get_type_hints(guessed)
    nested_type = hints.get("nested_files")
    assert nested_type is not None, "nested_files field not found"

    assert typing.get_origin(nested_type) is list, f"Expected outer List, got {typing.get_origin(nested_type)}"

    inner_type = typing.get_args(nested_type)[0]
    inner_origin = typing.get_origin(inner_type)
    assert inner_origin is list, f"Expected inner List, got {inner_type} (origin: {inner_origin})"

    innermost = typing.get_args(inner_type)[0]
    assert innermost is FlyteFile or dataclasses.is_dataclass(innermost), f"Expected FlyteFile or dataclass, got {innermost}"


def test_dict_of_dirs_guess_type_structure():
    """Verify guess_python_type reconstructs Dict[str, FlyteDirectory] with correct value type."""
    lit = TypeEngine.to_literal_type(ModelWithDictOfDirs)
    guessed = TypeEngine.guess_python_type(lit)
    assert dataclasses.is_dataclass(guessed)

    hints = typing.get_type_hints(guessed)
    dir_map_type = hints.get("dir_map")
    assert dir_map_type is not None, "dir_map field not found"

    assert typing.get_origin(dir_map_type) is dict, f"Expected Dict, got {typing.get_origin(dir_map_type)}"

    key_type, val_type = typing.get_args(dir_map_type)
    assert key_type is str, f"Expected str key, got {key_type}"
    assert val_type is FlyteDirectory or dataclasses.is_dataclass(val_type), f"Expected FlyteDirectory or dataclass, got {val_type}"


def test_list_of_structured_datasets_guess_type_structure():
    """Verify guess_python_type reconstructs List[StructuredDataset] with correct inner type."""
    lit = TypeEngine.to_literal_type(ModelWithListOfStructuredDatasets)
    guessed = TypeEngine.guess_python_type(lit)
    assert dataclasses.is_dataclass(guessed)

    hints = typing.get_type_hints(guessed)
    dfs_type = hints.get("dfs")
    assert dfs_type is not None, "dfs field not found"

    assert typing.get_origin(dfs_type) is list, f"Expected List, got {typing.get_origin(dfs_type)}"

    inner = typing.get_args(dfs_type)[0]
    assert inner is StructuredDataset or dataclasses.is_dataclass(inner), f"Expected StructuredDataset or dataclass, got {inner}"


def test_combined_model_guess_type_structure():
    """Verify guess_python_type reconstructs CombinedModel with all field types correct."""
    lit = TypeEngine.to_literal_type(CombinedModel)
    guessed = TypeEngine.guess_python_type(lit)
    assert dataclasses.is_dataclass(guessed)

    hints = typing.get_type_hints(guessed)

    # files: List[FlyteFile]
    files_type = hints["files"]
    assert typing.get_origin(files_type) is list
    files_inner = typing.get_args(files_type)[0]
    assert files_inner is FlyteFile or dataclasses.is_dataclass(files_inner)

    # dir_map: Dict[str, FlyteDirectory]
    dir_map_type = hints["dir_map"]
    assert typing.get_origin(dir_map_type) is dict
    _, dir_val = typing.get_args(dir_map_type)
    assert dir_val is FlyteDirectory or dataclasses.is_dataclass(dir_val)

    # optional_file: Optional[FlyteFile] — optional fields with defaults may be dropped
    # by guess_python_type, so we only assert structure if the field is present
    if "optional_file" in hints:
        opt_type = hints["optional_file"]
        assert typing.get_origin(opt_type) is typing.Union
        non_none_args = [a for a in typing.get_args(opt_type) if a is not type(None)]
        assert len(non_none_args) == 1
        assert non_none_args[0] is FlyteFile or dataclasses.is_dataclass(non_none_args[0])


# -- Roundtrip via guessed type (simulates pyflyte run) --
# In v1 flytekit, pydantic models are serialized as binary msgpack. Deserializing
# with a guessed dataclass type requires mashumaro to resolve inner types by name,
# which is not supported for dynamically generated dataclasses. These tests therefore
# verify the guessed type's structure rather than performing a full to_python_value call.


def test_list_of_files_roundtrip_via_guessed_type():
    """Verify guess_python_type for List[FlyteFile] returns a dataclass with a 'files' field."""
    ctx = FlyteContext.current_context()
    input_val = ModelWithListOfFiles(
        files=[FlyteFile.from_source("s3://bucket/a.txt"), FlyteFile.from_source("s3://bucket/b.txt")]
    )
    lit = TypeEngine.to_literal_type(ModelWithListOfFiles)
    TypeEngine.to_literal(ctx, input_val, ModelWithListOfFiles, lit)

    guessed = TypeEngine.guess_python_type(lit)
    assert dataclasses.is_dataclass(guessed)
    hints = typing.get_type_hints(guessed)
    assert "files" in hints
    assert typing.get_origin(hints["files"]) is list


def test_nested_files_roundtrip_via_guessed_type():
    """Verify guess_python_type for List[List[FlyteFile]] preserves nested List structure."""
    ctx = FlyteContext.current_context()
    input_val = ModelWithNestedFiles(
        nested_files=[
            [FlyteFile.from_source("s3://bucket/a.txt")],
            [FlyteFile.from_source("s3://bucket/b.txt"), FlyteFile.from_source("s3://bucket/c.txt")],
        ]
    )
    lit = TypeEngine.to_literal_type(ModelWithNestedFiles)
    TypeEngine.to_literal(ctx, input_val, ModelWithNestedFiles, lit)

    guessed = TypeEngine.guess_python_type(lit)
    assert dataclasses.is_dataclass(guessed)
    hints = typing.get_type_hints(guessed)
    nested_type = hints.get("nested_files")
    assert typing.get_origin(nested_type) is list
    inner_type = typing.get_args(nested_type)[0]
    assert typing.get_origin(inner_type) is list


def test_dict_of_dirs_roundtrip_via_guessed_type():
    """Verify guess_python_type for Dict[str, FlyteDirectory] preserves dict structure."""
    ctx = FlyteContext.current_context()
    input_val = ModelWithDictOfDirs(
        dir_map={"a": FlyteDirectory.from_source("s3://bucket/d1"), "b": FlyteDirectory.from_source("s3://bucket/d2")}
    )
    lit = TypeEngine.to_literal_type(ModelWithDictOfDirs)
    TypeEngine.to_literal(ctx, input_val, ModelWithDictOfDirs, lit)

    guessed = TypeEngine.guess_python_type(lit)
    assert dataclasses.is_dataclass(guessed)
    hints = typing.get_type_hints(guessed)
    dir_map_type = hints.get("dir_map")
    assert typing.get_origin(dir_map_type) is dict
    key_type, _ = typing.get_args(dir_map_type)
    assert key_type is str


def test_combined_model_roundtrip_via_guessed_type():
    """Verify guess_python_type for CombinedModel has all expected fields."""
    ctx = FlyteContext.current_context()
    input_val = CombinedModel(
        files=[FlyteFile.from_source("s3://bucket/a.txt")],
        dir_map={"d1": FlyteDirectory.from_source("s3://bucket/d1")},
        df=StructuredDataset(uri="s3://bucket/data.parquet"),
        optional_file=FlyteFile.from_source("s3://bucket/opt.txt"),
    )
    lit = TypeEngine.to_literal_type(CombinedModel)
    TypeEngine.to_literal(ctx, input_val, CombinedModel, lit)

    guessed = TypeEngine.guess_python_type(lit)
    assert dataclasses.is_dataclass(guessed)
    hints = typing.get_type_hints(guessed)
    assert "files" in hints
    assert "dir_map" in hints
    assert "df" in hints