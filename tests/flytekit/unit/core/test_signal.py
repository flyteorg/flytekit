
from flyteidl.admin.signal_pb2 import SignalSetRequest
from flytekit.core.type_engine import TypeEngine
from flytekit.core.context_manager import FlyteContextManager
from flytekit.clients.raw import RawSynchronousFlyteClient
from flytekit.configuration import PlatformConfig

def test_fjdska():
    ctx = FlyteContextManager.current_context()
    literal = TypeEngine.to_literal(ctx, 42, int, TypeEngine.to_literal_type(int))

    pc = PlatformConfig.auto(config_file="/Users/user/.flyte/config-file.yaml")
    cl = RawSynchronousFlyteClient(pc)
    ssr = SignalSetRequest(id="myid", value=literal)


def test_fdsdjfsd():
    SignalSetRequest()

