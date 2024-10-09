
Þ
SYSTEM:UnknownÇTraceback (most recent call last):

      File "/Users/ketanumare/src/flytekit/flytekit/exceptions/scopes.py", line 183, in system_entry_point
        return wrapped(*args, **kwargs)
      File "/Users/ketanumare/src/flytekit/flytekit/core/base_task.py", line 733, in dispatch_execute
        native_outputs = self.execute(**native_inputs)
      File "/Users/ketanumare/src/flytekit/flytekit/core/python_function_task.py", line 206, in execute
        return self.dynamic_execute(self._task_function, **kwargs)
      File "/Users/ketanumare/src/flytekit/flytekit/core/python_function_task.py", line 358, in dynamic_execute
        return self.compile_into_workflow(ctx, task_function, **kwargs)
      File "/Users/ketanumare/src/flytekit/flytekit/core/python_function_task.py", line 257, in compile_into_workflow
        workflow_spec: admin_workflow_models.WorkflowSpec = get_serializable(
      File "/Users/ketanumare/src/flytekit/flytekit/tools/translator.py", line 834, in get_serializable
        cp_entity = get_serializable_workflow(entity_mapping, settings, entity, options)
      File "/Users/ketanumare/src/flytekit/flytekit/tools/translator.py", line 341, in get_serializable_workflow
        serialized_nodes.append(get_serializable(entity_mapping, settings, n, options))
      File "/Users/ketanumare/src/flytekit/flytekit/tools/translator.py", line 837, in get_serializable
        cp_entity = get_serializable_node(entity_mapping, settings, entity, options)
      File "/Users/ketanumare/src/flytekit/flytekit/tools/translator.py", line 556, in get_serializable_node
        task_spec = get_serializable(entity_mapping, settings, entity.flyte_entity, options=options)
      File "/Users/ketanumare/src/flytekit/flytekit/tools/translator.py", line 831, in get_serializable
        cp_entity = get_serializable_task(entity_mapping, settings, entity, options)
      File "/Users/ketanumare/src/flytekit/flytekit/tools/translator.py", line 272, in get_serializable_task
        container = entity.get_container(settings)
      File "/Users/ketanumare/src/flytekit/flytekit/core/python_auto_container.py", line 199, in get_container
        return self._get_container(settings)
      File "/Users/ketanumare/src/flytekit/flytekit/core/python_auto_container.py", line 209, in _get_container
        args=self.get_command(settings=settings),
      File "/Users/ketanumare/src/flytekit/flytekit/core/python_auto_container.py", line 185, in get_command
        return self._get_command_fn(settings)
      File "/Users/ketanumare/src/flytekit/flytekit/core/python_auto_container.py", line 158, in get_default_command
        self.task_resolver.location,
      File "/Users/ketanumare/src/flytekit/flytekit/core/tracker.py", line 112, in location
        n, _, _, _ = extract_task_module(self)
      File "/Users/ketanumare/src/flytekit/flytekit/core/tracker.py", line 318, in extract_task_module
        else:
      File "/Users/ketanumare/src/flytekit/flytekit/core/tracker.py", line 120, in lhs
      File "/Users/ketanumare/src/flytekit/flytekit/core/tracker.py", line 120, in lhs
      File "/opt/homebrew/Cellar/python@3.9/3.9.19/Frameworks/Python.framework/Versions/3.9/lib/python3.9/bdb.py", line 88, in trace_dispatch
        return self.dispatch_line(frame)
      File "/opt/homebrew/Cellar/python@3.9/3.9.19/Frameworks/Python.framework/Versions/3.9/lib/python3.9/bdb.py", line 113, in dispatch_line
        if self.quitting: raise BdbQuit

Message:

    BdbQuit: 

SYSTEM ERROR! Contact platform administrators. 