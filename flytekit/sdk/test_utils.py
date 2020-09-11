from wrapt import decorator as _decorator

from flytekit.common import utils as _utils
from flytekit.interfaces.data import data_proxy as _data_proxy


class LocalTestFileSystem(object):
    """
    Context manager for creating a temporary test file system locally for the purpose of unit testing and grabbing
    remote objects.  This scratch space will be automatically cleaned up as long as sys.exit() is not called from
    within the context.  This context need only be used in user scripts and tests--all task executions are guaranteed
    to have the necessary managed disk context available.

    .. note::

        This is especially useful when dealing with remote blob-like objects (Blob, CSV, MultiPartBlob,
        MultiPartCSV, Schema) as they require backing on disk.  Using this context manager creates that disk context
        to support the downloads.

    .. note::

        Blob-like objects can be downloaded to user-specified locations.  See documentation for
        flytekit.sdk.types.Types for more information.

    .. note::

        When this context is entered, it overrides any contexts already entered.  All blobs will be written to the most
        recent entered context. Upon exiting the context, all data associated will be deleted.  It is recommended to
        only use one LocalTestFileSystem() per test to avoid confusion.

    .. code-block:: python

        with LocalTestFileSystem():
            wf_handle = SdkWorkflowExecution.fetch('project', 'domain', 'name')
            with wf_handle.node_executions['my_node'].outputs.blob as reader:
                assert reader.read() == "hello!"
    """

    def __init__(self):
        self._exit_stack = _utils.ExitStack()

    def __enter__(self):
        """
        :rtype: flytekit.common.utils.AutoDeletingTempDir
        """
        self._exit_stack.__enter__()
        temp_dir = self._exit_stack.enter_context(_utils.AutoDeletingTempDir("local_test_filesystem"))
        self._exit_stack.enter_context(_data_proxy.LocalDataContext(temp_dir.name))
        self._exit_stack.enter_context(_data_proxy.LocalWorkingDirectoryContext(temp_dir))
        return temp_dir

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._exit_stack.__exit__(exc_type, exc_val, exc_tb)


@_decorator
def flyte_test(fn, _, args, kwargs):
    """
    This is a decorator which can be used to annotate test functions.  By using this decorator, the necessary local
    scratch context will be prepared and then cleaned up upon completion.

    .. code-block:: python

        @inputs(input_blob=Types.Blob)
        @outputs(response_blob=Types.Blob)
        @python_task
        def test_task(wf_params, input_blob, response_blob):
            response = Types.Blob()
            with response as writer:
                with input_blob as reader:
                    txt = reader.read()
                    if txt == "Hi":
                        writer.write("Hello, world!")
                    elif txt == "Goodnight":
                        writer.write("Goodnight, moon.")
                    else:
                        writer.write("Does not compute".)
            response_blob.set(response)

        @flyte_test
        def some_test():
            blob = Types.Blob()
            with blob as writer:
                writer.write("Hi")

            result = test_task.unit_test(input_blob=blob)

            with result['response_blob'] as reader:
                assert reader.read() == 'Hello, world!"
    """
    with LocalTestFileSystem():
        return fn(*args, **kwargs)
