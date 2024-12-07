import os
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest

import flytekit
from flytekit.core.tracker import import_module_from_file
from flytekit.tools.script_mode import compress_scripts, hash_file, add_imported_modules_from_source, get_all_modules, \
    list_all_files
from flytekit.tools.script_mode import (
    list_imported_modules_as_files,
)

MAIN_WORKFLOW = """
from flytekit import task, workflow
from wf1.test import t1

@workflow
def my_wf() -> str:
    return "hello world"
"""

IMPERATIVE_WORKFLOW = """
from flytekit import Workflow, task

@task
def t1(a: int):
    print(a)


wf = Workflow(name="my.imperative.workflow.example")
wf.add_workflow_input("a", int)
node_t1 = wf.add_entity(t1, a=wf.inputs["a"])
"""

T1_TASK = """
from flytekit import task
from wf2.test import t2


@task()
def t1() -> str:
    print("hello")
    return "hello"
"""

T2_TASK = """
from flytekit import task

@task()
def t2() -> str:
    print("hello")
    return "hello"
"""


def test_deterministic_hash(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()

    # Create dummy init file
    open(workflows_dir / "__init__.py", "a").close()
    # Write a dummy workflow
    workflow_file = workflows_dir / "hello_world.py"
    workflow_file.write_text(MAIN_WORKFLOW)

    imperative_workflow_file = workflows_dir / "imperative_wf.py"
    imperative_workflow_file.write_text(IMPERATIVE_WORKFLOW)

    t1_dir = tmp_path / "wf1"
    t1_dir.mkdir()
    open(t1_dir / "__init__.py", "a").close()
    t1_file = t1_dir / "test.py"
    t1_file.write_text(T1_TASK)

    t2_dir = tmp_path / "wf2"
    t2_dir.mkdir()
    open(t2_dir / "__init__.py", "a").close()
    t2_file = t2_dir / "test.py"
    t2_file.write_text(T2_TASK)

    destination = tmp_path / "destination"

    modules = [
        import_module_from_file("workflows.hello_world", os.fspath(workflow_file)),
        import_module_from_file("workflows.imperative_wf", os.fspath(workflow_file)),
        import_module_from_file("wf1.test", os.fspath(t1_file)),
        import_module_from_file("wf2.test", os.fspath(t2_file))
    ]

    compress_scripts(str(workflows_dir.parent), str(destination), modules)

    digest, hex_digest, _ = hash_file(destination)

    # Try again to assert digest determinism
    destination2 = tmp_path / "destination2"
    compress_scripts(str(workflows_dir.parent), str(destination2), modules)
    digest2, hex_digest2, _ = hash_file(destination)

    assert digest == digest2
    assert hex_digest == hex_digest2

    test_dir = tmp_path / "test"
    test_dir.mkdir()

    result = subprocess.run(
        ["tar", "-xvf", destination, "-C", test_dir],
        stdout=subprocess.PIPE,
    )
    result.check_returncode()
    assert len(next(os.walk(test_dir))[1]) == 3

    compress_scripts(str(workflows_dir.parent), str(destination), "workflows.imperative_wf")


WORKFLOW_CONTENT = """
from flytekit import task, workflow
from utils import t1

@task
def my_task() -> str:
    return t1()

@workflow
def my_wf() -> str:
    return my_task()
"""

UTILS_CONTENT = """
def t1() -> str:
    return "hello world"
"""


def test_add_imported_modules_from_source_root_workflow(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    workflow_path = source_dir / "workflow.py"
    workflow_path.write_text(WORKFLOW_CONTENT)
    utils_path = source_dir / "utils.py"
    utils_path.write_text(UTILS_CONTENT)

    destination_dir = tmp_path / "dest"
    destination_dir.mkdir()

    module_workflow = import_module_from_file("workflow", os.fspath(workflow_path))
    module_utils = import_module_from_file("utils", os.fspath(utils_path))
    modules = [module_workflow, module_utils]

    add_imported_modules_from_source(os.fspath(source_dir), os.fspath(destination_dir), modules)

    workflow_dest = destination_dir / "workflow.py"
    utils_dest = destination_dir / "utils.py"

    assert workflow_dest.exists()
    assert utils_dest.exists()

    assert workflow_dest.read_text() == WORKFLOW_CONTENT
    assert utils_dest.read_text() == UTILS_CONTENT


WORKFLOW_NESTED_CONTENT = """
from flytekit import task, workflow
from my_workflows.utils import t1

@task
def my_task() -> str:
    return t1()

@workflow
def my_wf() -> str:
    return my_task()
"""

UTILS_NESTED_CONTENT_1 = """
from my_workflows.nested.utils import t2

def t1() -> str:
    return t2()
"""

UTILS_NESTED_CONTENT_2 = """
def t2() -> str:
    return "hello world"
"""


def test_add_imported_modules_from_source_nested_workflow(tmp_path):
    source_dir = tmp_path / "source"
    workflow_dir = source_dir / "my_workflows"
    workflow_dir.mkdir(parents=True)

    init_path = workflow_dir / "__init__.py"
    init_path.touch()

    workflow_path = workflow_dir / "main.py"
    workflow_path.write_text(WORKFLOW_NESTED_CONTENT)
    utils_path = workflow_dir / "utils.py"
    utils_path.write_text(UTILS_NESTED_CONTENT_1)

    nested_workflow = workflow_dir / "nested"
    nested_workflow.mkdir()
    nested_init = nested_workflow / "__init__.py"
    nested_init.touch()

    nested_utils = nested_workflow / "utils.py"
    nested_utils.write_text(UTILS_NESTED_CONTENT_2)

    destination_dir = tmp_path / "dest"
    destination_dir.mkdir()

    module_workflow = import_module_from_file("my_workflows.main", os.fspath(workflow_path))
    module_utils = import_module_from_file("my_workflows.utils", os.fspath(utils_path))
    module_nested_utils = import_module_from_file("my_workflows.nested.utils", os.fspath(nested_utils))
    modules = [module_workflow, module_utils, module_nested_utils]

    add_imported_modules_from_source(os.fspath(source_dir), os.fspath(destination_dir), modules)

    workflow_dest = destination_dir / "my_workflows" / "main.py"
    utils_1_dest = destination_dir / "my_workflows" / "utils.py"
    utils_2_dest = destination_dir / "my_workflows" / "nested" / "utils.py"

    assert workflow_dest.exists()
    assert utils_1_dest.exists()
    assert utils_2_dest.exists()

    assert workflow_dest.read_text() == WORKFLOW_NESTED_CONTENT
    assert utils_1_dest.read_text() == UTILS_NESTED_CONTENT_1
    assert utils_2_dest.read_text() == UTILS_NESTED_CONTENT_2


def test_get_all_modules(tmp_path):
    source_dir = tmp_path / "source"
    workflow_dir = source_dir / "my_workflows"
    workflow_dir.mkdir(parents=True)
    workflow_file = workflow_dir / "main.py"

    # workflow_file does not exists so there are no additional imports
    n_sys_modules = len(sys.modules)
    assert n_sys_modules == len(get_all_modules(os.fspath(source_dir), "my_workflows.main"))

    # Workflow exists, so it is imported
    workflow_file.write_text(WORKFLOW_CONTENT)
    assert n_sys_modules + 1 == len(get_all_modules(os.fspath(source_dir), "my_workflows.main"))


@patch("flytekit.tools.script_mode.sys")
@patch("site.getsitepackages")
def test_list_imported_modules_as_files(mock_getsitepackage, mock_sys, tmp_path):

    bin_directory = Path(os.path.dirname(sys.executable))
    flytekit_root = Path(os.path.dirname(flytekit.__file__))
    source_path = tmp_path / "project"

    # Site packages should be executed
    site_packages = [
        str(source_path / ".venv" / "lib" / "python3.10" / "site-packages"),
        str(source_path / ".venv" / "local" / "lib" / "python3.10" / "dist-packages"),
        str(source_path / ".venv" / "lib" / "python3" / "dist-packages"),
        str(source_path / ".venv" / "lib" / "python3.10" / "dist-packages"),
    ]
    mock_getsitepackage.return_value = site_packages

    # lib module that should be excluded, even if it is in the same roto as source_path
    lib_path = source_path / "micromamba" / "envs" / "my-env"
    lib_modules = [
        (ModuleType("lib_module"), str(lib_path / "module.py"))
    ]
    # mock the sys prefix to be in the source path
    mock_sys.prefix = str(lib_path)

    # bin module that should be excluded
    bin_modules = [
        (ModuleType("bin_module"), str(bin_directory / "bin" / "module.py"))
    ]
    # site modules that should be excluded
    site_modules = [
        (ModuleType("site_module_1"), str(Path(site_packages[0]) / "package" / "module_1.py")),
        (ModuleType("site_module_2"), str(Path(site_packages[1]) / "package" / "module_2.py")),
        (ModuleType("site_module_3"), str(Path(site_packages[2]) / "package" / "module_3.py")),
        (ModuleType("site_module_4"), str(Path(site_packages[3]) / "package" / "module_4.py")),
    ]

    # local modules that should be included
    local_modules = [
        (ModuleType("local_module_1"), str(source_path / "package_a" / "module_1.py")),
        (ModuleType("local_module_2"), str(source_path / "package_a" / "module_2.py")),
        (ModuleType("local_module_3"), str(source_path / "package_b" / "module_3.py")),
        (ModuleType("local_module_4"), str(source_path / "package_b" / "module_4.py")),
    ]
    flyte_modules = [
        (ModuleType("flyte_module"), str(flytekit_root / "package" / "module.py"))
    ]

    module_path_pairs = local_modules + flyte_modules + bin_modules + lib_modules + site_modules

    for m, p in module_path_pairs:
        m.__file__ = p

    modules = [m for m, _ in module_path_pairs]

    file_list = list_imported_modules_as_files(str(source_path), modules)

    assert sorted(file_list) == sorted([p for _, p in local_modules])


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Skip if running on windows since Unix Domain Sockets do not exist in that OS",
)
def test_list_all_files_skip_socket_files():
    tmp_dir = Path(tempfile.mkdtemp())

    source_dir = tmp_dir / "source"
    source_dir.mkdir()

    file1 = source_dir / "file1.py"
    file1.write_text("")

    socket_file = source_dir / "test.socket"
    server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_socket.bind(os.fspath(socket_file))

    files = list(list_all_files(os.fspath(source_dir), False))

    # Ensure that the socket file is not in the list of files
    assert str(socket_file) not in files

    # Ensure that the regular file is the only file in the list
    assert len(files) == 1
    assert str(file1) in files
