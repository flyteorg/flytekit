import re
from typing import Any, Dict, Union

import yaml
from docstring_parser import parse as _parse
from docstring_parser.common import Docstring


def parse_docstring(x: Union[str, Any]) -> Docstring:
    """ """

    if isinstance(x, str):
        return _parse(x)

    if not hasattr(x, "__doc__"):
        raise ValueError(f"No docstring found: '{x}'")

    return _parse(x.__doc__)


_yaml_metadata_regex = re.compile(r"(.*)__metadata__:(.*)", re.MULTILINE | re.DOTALL)


def param_metadata_from_docstring(doc: Docstring) -> Dict[str, Dict[str, Any]]:
    """Extracts structured metadata from docstrings.

    The following pieces of information are extracted from the body:

        * short descrption text
        * long description text
        * YAML serialized long description metadata

    The following pieces of information are extracted from each parameter
    described in the docstring

        * name
        * description
        * index/order
        * default value
        * YAML serialized parameter description metadata

    An example:

    .. code-block:: python

        @task
        def foo():
            ""<short description>

            <long description>

            __metadata__:
                <YAML serialized long description metadata>

            Args:
                <param name>:
                    <param description>

                    __metadata__:
                        <YAML serialized param description metadata>
            ""
    """

    res = {}
    for idx, x in enumerate(doc.params):
        meta = _yaml_metadata_regex.match(x.description)

        if meta is not None:
            try:
                res[x.arg_name] = {
                    "idx": idx,
                    "name": x.arg_name,
                    "default": x.default,
                    "description": meta.group(1),
                    "meta": yaml.safe_load(meta.group(2)),
                }
            except:
                pass

        if x.arg_name not in res:
            res[x.arg_name] = {"idx": idx, "name": x.arg_name, "default": x.default, "description": x.description}

        if idx == 0:
            if doc.long_description is not None:
                meta = _yaml_metadata_regex.match(doc.long_description)
                if meta is not None:
                    try:
                        res[x.arg_name]["__workflow_meta__"] = {
                            "short_description": doc.short_description,
                            "long_description": meta.group(1),
                            "meta": yaml.safe_load(meta.group(2)),
                        }
                    except:
                        pass

            if "__workflow_meta__" not in res[x.arg_name]:
                res[x.arg_name]["__workflow_meta__"] = {
                    "short_description": doc.short_description,
                    "long_description": doc.long_description,
                }

    return res


def returns_metadata_from_docstring(doc: Docstring) -> Dict[str, Dict[str, Any]]:
    res = {}
    for idx, x in enumerate(doc.many_returns):
        meta = _yaml_metadata_regex.match(x.description)
        if meta is not None:
            try:
                tmp = {
                    "idx": idx,
                    "name": x.return_name,
                    "description": meta.group(1),
                    "meta": yaml.safe_load(meta.group(2)),
                }
                res[x.return_name] = tmp
                continue
            except Exception as e:
                print(e)
                pass

        res[x.return_name] = {"idx": idx, "name": x.return_name, "description": x.description}

    return res
