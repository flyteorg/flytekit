from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from flyteidl.admin import entity_description_pb2 as _entity_description_pb2

from flytekit.models import common as _common_models


@dataclass
class LongDescription(_common_models.FlyteIdlEntity):
    class DescriptionFormat(Enum):
        UNKNOWN = 0
        MARKDOWN = 1
        HTML = 2
        RST = 3

    values: Optional[str] = ""
    uri: Optional[str] = ""
    icon_link: Optional[str] = ""
    format: DescriptionFormat = DescriptionFormat.RST

    @classmethod
    def from_flyte_idl(cls, pb2_object: _entity_description_pb2.LongDescription) -> "LongDescription":
        return cls(
            values=pb2_object.values,
            uri=pb2_object.uri,
            format=LongDescription.DescriptionFormat(pb2_object.long_format),
            icon_link=pb2_object.icon_link,
        )

    def to_flyte_idl(self):
        return _entity_description_pb2.LongDescription(
            values=self.values, uri=self.uri, long_format=self.format.value, icon_link=self.icon_link
        )


@dataclass
class SourceCode(_common_models.FlyteIdlEntity):
    file: Optional[str] = None
    line_number: Optional[int] = None
    repo: Optional[str] = None
    branch: Optional[str] = None
    link: Optional[str] = None
    language: Optional[str] = None

    @classmethod
    def from_flyte_idl(cls, pb2_object: _entity_description_pb2.SourceCode) -> "SourceCode":
        return cls(
            file=pb2_object.file,
            line_number=pb2_object.line_number,
            repo=pb2_object.repo,
            branch=pb2_object.branch,
            link=pb2_object.link,
            language=pb2_object.language,
        )

    def to_flyte_idl(self):
        return _entity_description_pb2.SourceCode(
            file=self.file,
            line_number=self.line_number,
            repo=self.repo,
            branch=self.branch,
            link=self.link,
            language=self.language,
        )


@dataclass
class Documentation(_common_models.FlyteIdlEntity):

    short_description: str
    long_description: LongDescription
    source_code: Optional[SourceCode] = None
    tags: Optional[List[str]] = None
    labels: Optional[Dict[str, str]] = None

    @classmethod
    def from_flyte_idl(cls, pb2_object: _entity_description_pb2.EntityDescription) -> "Documentation":
        return cls(
            short_description=pb2_object.short_description,
            long_description=LongDescription.from_flyte_idl(pb2_object.long_description),
            source_code=SourceCode.from_flyte_idl(pb2_object.source_code),
            tags=pb2_object.tags,
            labels=pb2_object.labels,
        )

    def to_flyte_idl(self):
        return _entity_description_pb2.EntityDescription(
            short_description=self.short_description,
            long_description=self.long_description.to_flyte_idl() if self.long_description else None,
            tags=self.tags,
            labels=self.labels,
            source=self.source_code.to_flyte_idl() if self.source_code else None,
        )
