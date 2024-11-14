from flytekit.configuration import Config, SerializationSettings
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.task import task
from flytekit.experimental.eager_function import eager
from flytekit.remote.remote import FlyteRemote
from flytekit.utils.asyn import loop_manager


@task
def add_one(x: int) -> int:
    return x + 1


@eager
async def simple_eager_workflow(x: int) -> int:
    # This is the normal way of calling tasks. Call normal tasks in an effectively async way by hanging and waiting for
    # the result.
    out = add_one(x=x)
    return out


def main():
    ctx = FlyteContextManager.current_context()
    ss = SerializationSettings.from_transport(SS_CTX)
    remote = FlyteRemote(Config.for_sandbox())
    dc = Config.for_sandbox().data_config
    raw_output = "s3://my-s3-bucket/testing/async_test/raw_output/"
    print(f"Using raw output location: {raw_output}")
    provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output, data_config=dc)

    with FlyteContextManager.with_context(
        ctx.with_file_access(provider).with_client(remote.client).with_serialization_settings(ss)
    ) as ctx:
        res = loop_manager.run_sync(simple_eager_workflow.run_with_backend, ctx, x=1)
        print(res)
        assert res == 42


if __name__ == "__main__":
    SS_CTX = "H4sIAAAAAAAC/72Rb2vbMBDGv0rw68XC9dL8gb0oHRsJNIOxlZUxzFk6K1pkyZNOLk7pd6+kZFv3BeYX5nTPT3fPnZ4K1YPEhlvTKVlsZk+FwA6CpiYLOWOgT8FvpXgzK7pfJmW4Kzs9EZbWSZajP8FRZZAgVS2GqS6raq6B0Oe8UDJFm5kJWj/HRG7nY+L7/2j4I7UcnP2JPOWKXMEb4EefYduDMmcLI2o79GhylRGdVzYr7aPvTgJWsL0/PG63b7uqWnUyQWjGS594kIoah4NNV5I4THSwplGG0A0O4z8pzA7ExniRtcqwM1PnuS+jNaNyFEBHpHHW0j+XMgieGo9OgVYnoGgynoiUkT4/IhpoNYoYkwuYZozrUOZMCnV2kSvnZXlyqg1Z1JZnKhG+3jDWT3Nfz9vAj0js1ebYq2WxXX338G13s7h9+LC///z+5tOX/cfd7f7qbvsufywZvr6+Wq05LBFALKGtBIrlYi1wUa/Eul0vSgJXylORXsvb4Dj+Hf5rHNaziayRTFrmHWdx14fQltz2LJjoGFS01AY5EQuRvsCRLJ5fAEGB/PL5AgAA"
    main()
