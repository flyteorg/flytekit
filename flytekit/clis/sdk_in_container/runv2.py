from __future__ import annotations
import importlib.util
import json
import sys
from typing import Any, Dict

import click
import flyte
import flytekit

from flytekit.migration.task import task_shim
from flytekit.migration.workflow import workflow_shim


def _load_module_from_path(path: str):
    spec = importlib.util.spec_from_file_location("user_module", path)
    if spec is None or spec.loader is None:
        raise click.UsageError(f"Cannot load module from: {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["user_module"] = mod
    spec.loader.exec_module(mod)
    return mod


def _parse_kv(pairs: tuple[str, ...]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for kv in pairs:
        if "=" not in kv:
            raise click.UsageError(f"Bad input '{kv}', expected key=value")
        k, v = kv.split("=", 1)
        # naive coercion
        if v.lower() in {"true", "false"}:
            out[k] = (v.lower() == "true")
        else:
            try:
                out[k] = int(v) if "." not in v else float(v)
            except ValueError:
                out[k] = v
    return out


def _run_remote(entity, inputs):
    if hasattr(flyte, "remote") and callable(getattr(flyte, "remote")):
        r = flyte.remote()
        try:
            return r.run(entity, **inputs)
        except TypeError:
            return r.run(entity, inputs=inputs)
    elif hasattr(flyte, "submit"):
        return flyte.submit(entity, **inputs)
    elif hasattr(flyte, "Runner") and hasattr(flyte.Runner, "remote"):
        rr = flyte.Runner.remote()
        try:
            return rr.run(entity, inputs)
        except TypeError:
            return rr.run(entity, **inputs)
    else:
        raise click.UsageError(
            "Remote execution is not available in this flyte-sdk build. Please upgrade flyte-sdk or configure a remote backend."
        )


@click.command("runv2", context_settings={"ignore_unknown_options": True})
@click.option("--remote", is_flag=True, default=False,
              help="Submit via Flyte 2 remote backend if configured; otherwise run locally.")
@click.argument("pyfile", type=click.Path(exists=True))
@click.argument("entity_name")
@click.option("-i", "--input", "inputs_kv", multiple=True, help="key=value pairs")
@click.option("--config", type=click.Path(exists=True), help="Flyte 2 SDK config file")
def runv2(pyfile: str, entity_name: str, inputs_kv: tuple[str, ...], config: str | None,remote: bool):
    """
    pyflyte runv2 xx.py <workflow_or_task_name> -i a=1 -i b=hello

    Loads the module, applies v2 shims to flytekit decorators, and executes
    the selected entity via the Flyte 2 runtime (flyte.run).
    """
    # init Flyte 2
    if config:
        flyte.init_from_config(config)
    else:
        flyte.init()

    flytekit.task = task_shim
    flytekit.workflow = workflow_shim

    spec = importlib.util.spec_from_file_location("user_module", pyfile)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["user_module"] = mod
    spec.loader.exec_module(mod)  # type: ignore

    entity = getattr(mod, entity_name)
    inputs = {}
    for kv in inputs_kv:
        k, v = kv.split("=", 1)
        inputs[k] = (v.lower() == "true") if v.lower() in ("true", "false") else (
            float(v) if "." in v else (int(v) if v.isdigit() else v))

    if remote:
        out = _run_remote(entity, inputs)
    else:
        out = flyte.run(entity, **inputs)

    value = out
    for name in ("result", "output", "outputs"):
        attr = getattr(value, name, None)
        if attr is None:
            continue
        try:
            value = attr() if callable(attr) else attr
        except TypeError:
            value = attr

    if isinstance(value, dict) and len(value) == 1:
        try:
            value = next(iter(value.values()))
        except Exception:
            pass

    try:
        click.echo(json.dumps({"result": value}, default=str))
    except Exception:
        click.echo(str(value))