




A custom task writer should have to write the following


- how to promote_from_model
- how to serialize to model


- given a task, what is the command
- there is a default command
- the default command should have code that downloads input things and uploads output things

- the
- things have to work locally


what does the writer care about?
- given inputs, how to run the task


inputs/outputs can be rehydrated from the thing.

- container by default calls dispatch execute


so what happens
at execution time, the command spins up, input paths are filled in. protos are by default downloaded,


need a custom task resolver


things we need to do
* serialize the task with a command different than the default (we need the task template)
