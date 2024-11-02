from flytekit.core.task import task
from flytekit.core.workflow import workflow


@task
def add_one(x: int) -> int:
    return x + 1


@task
async def double(x: int) -> int:
    return x * 2


@eager
async def simple_eager_workflow(x: int) -> int:
    # This is the normal way of calling tasks. Call normal tasks in an effectively async way by hanging and waiting for
    # the result.
    out = add_one(x=x)

    # This is the patter for
    doubled = double(x=x)
    other_double = double(x=x)
    doubled, other_double = asyncio.gather(doubled, other_double)
    if out - doubled < 0:
        return -1
    return await double(x=out)

    with setup_execution(
        raw_output_data_prefix,
        output_prefix,
        checkpoint_path,
        prev_checkpoint,
        dynamic_addl_distro,
        dynamic_dest_dir,
    ) as ctx:

        outputs = task_def.dispatch_execute(ctx, idl_input_literals)
