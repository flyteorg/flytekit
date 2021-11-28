import datetime
import logging
import os
import re
import subprocess
import typing
from dataclasses import dataclass

from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.interface import Interface
from flytekit.core.python_function_task import PythonInstanceTask
from flytekit.core.task import TaskPlugins
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile


@dataclass
class OutputLocation:
    """
    Args:
        var: str The name of the output variable
        var_type: typing.Type The type of output variable
        location: os.PathLike The location where this output variable will be written to or a regex that accepts input
                  vars and generates the path. Of the form ``"{{ .inputs.v }}.tmp.md"``.
                  This example for a given input v, at path `/tmp/abc.csv` will resolve to `/tmp/abc.csv.tmp.md`
    """

    var: str
    var_type: typing.Type
    location: typing.Union[os.PathLike, str]


def _stringify(v: typing.Any) -> str:
    """
    Special cased return for the given value. Given the type returns the string version for the type.
    Handles FlyteFile and FlyteDirectory specially. Downloads and returns the downloaded filepath
    """
    if isinstance(v, FlyteFile):
        v.download()
        return v.path
    if isinstance(v, FlyteDirectory):
        v.download()
        return v.path
    if isinstance(v, datetime.datetime):
        return v.isoformat()
    return str(v)


def _interpolate(tmpl: str, regex: re.Pattern, validate_all_match: bool = True, **kwargs) -> str:
    """
    Substitutes all templates that match the supplied regex
    with the given inputs and returns the substituted string. The result is non destructive towards the given string.
    """
    modified = tmpl
    matched = set()
    for match in regex.finditer(tmpl):
        expr = match.groups()[0]
        var = match.groups()[1]
        if var not in kwargs:
            raise ValueError(f"Variable {var} in Query (part of {expr}) not found in inputs {kwargs.keys()}")
        matched.add(var)
        val = kwargs[var]
        # str conversion should be deliberate, with right conversion for each type
        modified = modified.replace(expr, _stringify(val))

    if validate_all_match:
        if len(matched) < len(kwargs.keys()):
            diff = set(kwargs.keys()).difference(matched)
            raise ValueError(f"Extra Inputs have no matches in script template - missing {diff}")
    return modified


def _dummy_task_func():
    """
    A Fake function to satisfy the inner PythonTask requirements
    """
    return None


T = typing.TypeVar("T")


class ShellTask(PythonInstanceTask[T]):
    """ """

    _INPUT_REGEX = re.compile(r"({{\s*.inputs.(\w+)\s*}})", re.IGNORECASE)
    _OUTPUT_REGEX = re.compile(r"({{\s*.outputs.(\w+)\s*}})", re.IGNORECASE)

    def __init__(
        self,
        name: str,
        debug: bool = False,
        script: typing.Optional[str] = None,
        script_file: typing.Optional[str] = None,
        task_config: T = None,
        inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        output_locs: typing.Optional[typing.List[OutputLocation]] = None,
        **kwargs,
    ):
        """
        Args:
            name: str Name of the Task. Should be unique in the project
            debug: bool Print the generated script and other debugging information
            script: The actual script specified as a string
            script_file: A path to the file that contains the script (Only script or script_file) can be provided
            task_config: T Configuration for the task, can be either a Pod (or coming soon, BatchJob) config
            inputs: A Dictionary of input names to types
            output_locs: A list of :py:class:`OutputLocations`
            **kwargs: Other arguments that can be passed to :ref:class:`PythonInstanceTask`
        """
        if script and script_file:
            raise ValueError("Only either of script or script_file can be provided")
        if not script and not script_file:
            raise ValueError("Either a script or script_file is needed")
        if script_file:
            if not os.path.exists(script_file):
                raise ValueError(f"FileNotFound: the specified Script file at path {script_file} cannot be loaded")
            script_file = os.path.abspath(script_file)

        if task_config is not None:
            if str(type(task_config)) != "flytekitplugins.pod.task.Pod":
                raise ValueError("TaskConfig can either be empty - indicating simple container task or a PodConfig.")

        # Each instance of NotebookTask instantiates an underlying task with a dummy function that will only be used
        # to run pre- and post- execute functions using the corresponding task plugin.
        # We rename the function name here to ensure the generated task has a unique name and avoid duplicate task name
        # errors.
        # This seem like a hack. We should use a plugin_class that doesn't require a fake-function to make work.
        plugin_class = TaskPlugins.find_pythontask_plugin(type(task_config))
        self._config_task_instance = plugin_class(task_config=task_config, task_function=_dummy_task_func)
        # Rename the internal task so that there are no conflicts at serialization time. Technically these internal
        # tasks should not be serialized at all, but we don't currently have a mechanism for skipping Flyte entities
        # at serialization time.
        self._config_task_instance._name = f"_bash.{name}"
        self._script = script
        self._script_file = script_file
        self._debug = debug
        self._output_locs = output_locs if output_locs else []
        outputs = self._validate_output_locs()
        super().__init__(
            name,
            task_config,
            task_type=self._config_task_instance.task_type,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    def _validate_output_locs(self) -> typing.Dict[str, typing.Type]:
        outputs = {}
        for v in self._output_locs:
            if v is None:
                raise ValueError("OutputLocation cannot be none")
            if not isinstance(v, OutputLocation):
                raise ValueError("Every output type should be an output location on the file-system")
            if v.location is None:
                raise ValueError(f"Output Location not provided for output var {v.var}")
            if not issubclass(v.var_type, FlyteFile) and not issubclass(v.var_type, FlyteDirectory):
                raise ValueError(
                    "Currently only outputs of type FlyteFile/FlyteDirectory and their derived types are supported"
                )
            outputs[v.var] = v.var_type
        return outputs

    @property
    def script(self) -> typing.Optional[str]:
        return self._script

    @property
    def script_file(self) -> typing.Optional[os.PathLike]:
        return self._script_file

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        return self._config_task_instance.pre_execute(user_params)

    def execute(self, **kwargs) -> typing.Any:
        """
        Executes the given script by substituting the inputs and outputs and extracts the outputs from the filesystem
        """
        logging.info(f"Running shell script as type {self.task_type}")
        if self.script_file:
            with open(self.script_file) as f:
                self._script = f.read()

        outputs: typing.Dict[str, str] = {}
        if self._output_locs:
            for v in self._output_locs:
                outputs[v.var] = _interpolate(v.location, self._INPUT_REGEX, validate_all_match=False, **kwargs)

        gen_script = _interpolate(self._script, self._INPUT_REGEX, **kwargs)
        # For outputs it is not necessary that all outputs are used in the script, some are implicit outputs
        # for example gcc main.c will generate a.out automatically
        gen_script = _interpolate(gen_script, self._OUTPUT_REGEX, validate_all_match=False, **outputs)
        if self._debug:
            print("\n==============================================\n")
            print(gen_script)
            print("\n==============================================\n")

        try:
            subprocess.check_call(gen_script, shell=True)
        except subprocess.CalledProcessError as e:
            files = os.listdir("./")
            fstr = "\n-".join(files)
            logging.error(
                f"Failed to Execute Script, return-code {e.returncode} \n"
                f"StdErr: {e.stderr}\n"
                f"StdOut: {e.stdout}\n"
                f" Current directory contents: .\n-{fstr}"
            )
            raise

        final_outputs = []
        for v in self._output_locs:
            if issubclass(v.var_type, FlyteFile):
                final_outputs.append(FlyteFile(outputs[v.var]))
            if issubclass(v.var_type, FlyteDirectory):
                final_outputs.append(FlyteDirectory(outputs[v.var]))
        if len(final_outputs) == 1:
            return final_outputs[0]
        if len(final_outputs) > 1:
            return tuple(final_outputs)
        return None

    def post_execute(self, user_params: ExecutionParameters, rval: typing.Any) -> typing.Any:
        return self._config_task_instance.post_execute(user_params, rval)
