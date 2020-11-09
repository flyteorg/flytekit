import importlib
import pkgutil
from typing import List

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.local_workflow import SdkRunnableWorkflow as _SdkRunnableWorkflow
from flytekit.common.mixins import registerable as _registerable


def iterate_modules(pkgs):
    for package_name in pkgs:
        package = importlib.import_module(package_name)
        yield package
        for _, name, _ in pkgutil.walk_packages(package.__path__, prefix="{}.".format(package_name)):
            yield importlib.import_module(name)


def just_load_modules(pkgs: List[str]):
    """
    This one differs from the above in that we don't yield anything, just load all the modules.
    """
    for package_name in pkgs:
        package = importlib.import_module(package_name)
        for _, name, _ in pkgutil.walk_packages(package.__path__, prefix="{}.".format(package_name)):
            importlib.import_module(name)


def load_workflow_modules(pkgs):
    """
    Load all modules and packages at and under the given package.  Used for finding workflows/tasks to register.

    :param list[Text] pkgs: List of dot separated string containing paths folders (packages) containing
        the modules (python files)
    :raises ImportError
    """
    for _ in iterate_modules(pkgs):
        pass


def _topo_sort_helper(
    obj,
    entity_to_module_key,
    visited,
    recursion_set,
    recursion_stack,
    include_entities,
    ignore_entities,
    detect_unreferenced_entities,
):
    visited.add(obj)
    recursion_stack.append(obj)
    if obj in recursion_set:
        raise _user_exceptions.FlyteAssertion(
            "A cyclical dependency was detected during topological sort of entities.  "
            "Cycle path was:\n\n\t{}".format("\n\t".join(p for p in recursion_stack[recursion_set[obj] :]))
        )
    recursion_set[obj] = len(recursion_stack) - 1

    if isinstance(obj, _registerable.HasDependencies):
        for upstream in obj.upstream_entities:
            if upstream not in visited:
                for m1, k1, o1 in _topo_sort_helper(
                    upstream,
                    entity_to_module_key,
                    visited,
                    recursion_set,
                    recursion_stack,
                    include_entities,
                    ignore_entities,
                    detect_unreferenced_entities,
                ):
                    if not o1.has_registered:
                        yield m1, k1, o1

    recursion_stack.pop()
    del recursion_set[obj]

    if isinstance(obj, include_entities) or not isinstance(obj, ignore_entities):
        if obj in entity_to_module_key:
            yield entity_to_module_key[obj] + (obj,)
        elif detect_unreferenced_entities:
            raise _user_exceptions.FlyteAssertion(
                "An entity was not found in modules accessible from the workflow packages configuration.  Please "
                "ensure that entities in '{}' are moved to a configured packaged, or adjust the configuration.".format(
                    obj.instantiated_in
                )
            )


def iterate_registerable_entities_in_order(
    pkgs, ignore_entities=None, include_entities=None, detect_unreferenced_entities=True
):
    """
    This function will iterate all discovered entities in the given package list.  It will then attempt to
    topologically sort such that any entity with a dependency on another comes later in the list.  Note that workflows
    can reference other workflows and launch plans.
    :param list[Text] pkgs:
    :param set[type] ignore_entities: If specified, ignore these entities while doing a topological sort.  All other
        entities will be taken.  Only one of ignore_entities or include_entities can be set.
    :param set[type] include_entities: If specified, include these entities while doing a topological sort.  All
        other entities will be ignored.  Only one of ignore_entities or include_entities can be set.
    :param bool detect_unreferenced_entities: If true, we will raise exceptions on entities not included in the package
        configuration.
    :rtype: module, Text, flytekit.common.mixins.registerable.RegisterableEntity
    """
    if ignore_entities and include_entities:
        raise _user_exceptions.FlyteAssertion("ignore_entities and include_entities cannot both be set")
    elif not ignore_entities and not include_entities:
        include_entities = (object,)
        ignore_entities = tuple()
    else:
        ignore_entities = tuple(list(ignore_entities or set([object])))
        include_entities = tuple(list(include_entities or set()))

    entity_to_module_key = {}
    for m in iterate_modules(pkgs):
        for k in dir(m):
            o = m.__dict__[k]
            if isinstance(o, _registerable.RegisterableEntity) and not o.has_registered:
                if o.instantiated_in == m.__name__:
                    entity_to_module_key[o] = (m, k)
                    if isinstance(o, _SdkRunnableWorkflow) and o.should_create_default_launch_plan:
                        # SDK should create a default launch plan for a workflow.  This is a special-case to simplify
                        # authoring of workflows.
                        entity_to_module_key[o.create_launch_plan()] = (m, k)

    visited = set()
    for o in entity_to_module_key.keys():
        if o not in visited:
            recursion_set = dict()
            recursion_stack = []
            for m, k, o2 in _topo_sort_helper(
                o,
                entity_to_module_key,
                visited,
                recursion_set,
                recursion_stack,
                include_entities,
                ignore_entities,
                detect_unreferenced_entities=detect_unreferenced_entities,
            ):
                yield m, k, o2
