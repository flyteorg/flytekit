import logging as _logging
import os as _os
import shutil as _shutil
import tempfile as _tempfile
import time as _time
from hashlib import sha224 as _sha224
from pathlib import Path
import docutils.parsers.rst
from docutils.nodes import GenericNodeVisitor

import flytekit as _flytekit
from flytekit.configuration import sdk as _sdk_config
from flytekit.models.core import identifier as _identifier


def _dnsify(value):  # type: (str) -> str
    """
    Converts value into a DNS-compliant (RFC1035/RFC1123 DNS_LABEL). The resulting string must only consist of
    alphanumeric (lower-case a-z, and 0-9) and not exceed 63 characters. It's permitted to have '-' character as long
    as it's not in the first or last positions.

    :param Text value:
    :rtype: Text
    """
    res = ""
    MAX = 63
    HASH_LEN = 10
    if len(value) >= MAX:
        h = _sha224(value.encode("utf-8")).hexdigest()[:HASH_LEN]
        value = "{}-{}".format(h, value[-(MAX - HASH_LEN - 1) :])
    for ch in value:
        if ch == "_" or ch == "-" or ch == ".":
            # Convert '_' to '-' unless it's the first character, in which case we drop it.
            if res != "" and len(res) < 62:
                res += "-"
        elif not ch.isalnum():
            # Trim non-alphanumeric letters.
            pass
        elif ch.islower() or ch.isdigit():
            # Character is already compliant, just append it.
            res += ch
        else:
            # Character is upper-case. Add a '-' before it for better readability.
            if res != "" and res[-1] != "-" and len(res) < 62:
                res += "-"
            res += ch.lower()

    if len(res) > 0 and res[-1] == "-":
        res = res[: len(res) - 1]

    return res


def load_proto_from_file(pb2_type, path):
    with open(path, "rb") as reader:
        out = pb2_type()
        out.ParseFromString(reader.read())
        return out


def write_proto_to_file(proto, path):
    Path(_os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as writer:
        writer.write(proto.SerializeToString())


def get_version_message():
    return "Welcome to Flyte! Version: {}".format(_flytekit.__version__)


class Directory(object):
    def __init__(self, path):
        """
        :param Text path: local path of directory
        """
        self._name = path

    @property
    def name(self):
        """
        :rtype: Text
        """
        return self._name

    def list_dir(self):
        """
        The list of absolute filepaths for all immediate sub-paths
        :rtype: list[Text]
        """
        return [_os.path.join(self.name, f) for f in _os.listdir(self.name)]

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class AutoDeletingTempDir(Directory):
    """
    Creates a posix safe tempdir which is auto deleted once out of scope
    """

    def __init__(self, working_dir_prefix=None, tmp_dir=None, cleanup=True):
        """
        :param Text working_dir_prefix: A prefix to help identify temporary directories
        :param Text tmp_dir: Path to desired temporary directory
        :param bool cleanup: Whether the directory should be cleaned up upon exit
        """
        self._tmp_dir = tmp_dir
        self._working_dir_prefix = (working_dir_prefix + "_") if working_dir_prefix else ""
        self._cleanup = cleanup
        super(AutoDeletingTempDir, self).__init__(None)

    def __enter__(self):
        self._name = _tempfile.mkdtemp(dir=self._tmp_dir, prefix=self._working_dir_prefix)
        return self

    def get_named_tempfile(self, name):
        return _os.path.join(self.name, name)

    def _cleanup_dir(self):
        if self.name and self._cleanup:
            if _os.path.exists(self.name):
                _shutil.rmtree(self.name)
            self._name = None

    def force_cleanup(self):
        self._cleanup_dir()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup_dir()

    def __repr__(self):
        return "Auto-Deleting Tmp Directory @ {}".format(self.name)

    def __str__(self):
        return self.__repr__()


class PerformanceTimer(object):
    def __init__(self, context_statement):
        """
        :param Text context_statement: the statement to log
        """
        self._context_statement = context_statement
        self._start_wall_time = None
        self._start_process_time = None

    def __enter__(self):
        _logging.info("Entering timed context: {}".format(self._context_statement))
        self._start_wall_time = _time.perf_counter()
        self._start_process_time = _time.process_time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_wall_time = _time.perf_counter()
        end_process_time = _time.process_time()
        _logging.info(
            "Exiting timed context: {} [Wall Time: {}s, Process Time: {}s]".format(
                self._context_statement,
                end_wall_time - self._start_wall_time,
                end_process_time - self._start_process_time,
            )
        )


class ExitStack(object):
    def __init__(self, entered_stack=None):
        self._contexts = entered_stack

    def enter_context(self, context):
        out = context.__enter__()
        self._contexts.append(context)
        return out

    def pop_all(self):
        entered_stack = self._contexts
        self._contexts = None
        return ExitStack(entered_stack=entered_stack)

    def __enter__(self):
        if self._contexts is not None:
            raise Exception("A non-empty context stack cannot be entered.")
        self._contexts = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        first_exception = None
        if self._contexts is not None:
            while len(self._contexts) > 0:
                try:
                    self._contexts.pop().__exit__(exc_type, exc_val, exc_tb)
                except Exception as ex:
                    # Catch all to try to clean up all exits before re-raising the first exception
                    if first_exception is None:
                        first_exception = ex
        if first_exception is not None:
            raise first_exception
        return False


def fqdn(module, name, entity_type=None):
    """
    :param Text module:
    :param Text name:
    :param int entity_type: _identifier.ResourceType enum
    :rtype: Text
    """
    fmt = _sdk_config.NAME_FORMAT.get()
    if entity_type == _identifier.ResourceType.WORKFLOW:
        fmt = _sdk_config.WORKFLOW_NAME_FORMAT.get() or fmt
    elif entity_type == _identifier.ResourceType.TASK:
        fmt = _sdk_config.TASK_NAME_FORMAT.get() or fmt
    elif entity_type == _identifier.ResourceType.LAUNCH_PLAN:
        fmt = _sdk_config.LAUNCH_PLAN_NAME_FORMAT.get() or fmt
    return fmt.format(module=module, name=name)


def fqdn_safe(module, key, entity_type=None):
    """
    :param Text module:
    :param Text key:
    :param int entity_type: _identifier.ResourceType enum
    :rtype: Text
    """
    return _dnsify(fqdn(module, key, entity_type=entity_type))

# todo delete this
class Docstring(object):
    def __init__(self, raw_doc):  # fn.__doc__
        parser = docutils.parsers.rst.Parser()
        components = (docutils.parsers.rst.Parser,)
        settings = docutils.frontend.OptionParser(components=components).get_default_values()
        document = docutils.utils.new_document('<rst-doc>', settings=settings)
        parser.parse(raw_doc, document)
        visitor = FieldVisitor(document)
        document.walk(visitor)
        fields = visitor.get_fields_as_dict()
        self._inputs = {k[6:].split()[-1]: v for k, v in fields.items() if k.startswith('param ')}
        outputs = [v for k, v in fields.items() if k.startswith('return')]
        # not sure how to deal with return value for now
        if len(outputs) > 0:
            self._output = outputs[0]
        else:
            self._output = None

    def get_input_descriptions(self):
        return self._inputs

    def get_output_description(self):
        return self._output


class FieldVisitor(GenericNodeVisitor):
    def __init__(self, document):
        super().__init__(document)
        self._field_names = []
        self._field_bodies = []

    def default_departure(self, node):
        pass

    def visit_field_name(self, node):
        # assume raw text for now
        self._field_names.append(node[0].astext())

    def visit_field_body(self, node):
        # assume raw text for now
        # with none check
        self._field_bodies.append(node[0].astext() if len(node) > 0 else None)

    def default_visit(self, node):
        pass

    def get_fields_as_dict(self):
        # Use key name as default if value is None
        return {k: v if v else k for k, v in zip(self._field_names, self._field_bodies)}
