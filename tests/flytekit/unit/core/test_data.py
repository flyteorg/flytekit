import fsspec
import typing
from pyarrow import fs
from fsspec.implementations.arrow import ArrowFSWrapper

from flytekit.configuration import Config
from flytekit.core.data_persistence import FileAccessProvider

# pyarrow stuff
local = fs.LocalFileSystem()
local_fsspec = ArrowFSWrapper(local)


s3, path = fs.FileSystem.from_uri("s3://flyte-demo/datasets/sddemo/small.parquet")
print(s3, path)
f = s3.open_input_stream(path)
f.readall()
ws3 = ArrowFSWrapper(s3)

ss3 = fs.S3FileSystem(region='us-east-2')

# base fsspect stuff
fs3 = fsspec.filesystem("s3")
fs3.cat_file("s3://flyte-demo/datasets/sddemo/small.parquet")

# currently
# type(s3) => pyarrow._s3fs.S3FileSystem
#
# fs3 = fsspec.filesystem("s3")
# type(fs3) =>s3fs.core.S3FileSystem

# want to make it so that when you call fsspec.filesystem("s3") or anything else you get
# ws3 instead
# cls = filesystem(protocol, **storage_options)
# cls(**storage_options)
# need a class where when you do cls(**storage_options), the object that you get back is
#
# s3, path = fs.FileSystem.from_uri("s3://flyte-demo/datasets/sddemo/small.parquet")
# ws3 = ArrowFSWrapper(s3)

# but if s3 is created just like fs.S3FileSystem() without any region, and the region is incorrect,
# then this will not work.
# ws3.cat_file("s3://flyte-demo/datasets/sddemo/small.parquet")
# so s3 needs to be created correctly when there's a path.
# when is there a path - get data, put data, get filesystem


# class MyS3(object):
#     def __new__(cls, *args, **kwargs):
#         return ArrowFSWrapper(s3)
# fsspec.register_implementation("s3", MyS3)

# a class, such that when you do cls(), we get back exactly what we want.
# MyS3 does that, but then new() doesn't return the class itself.

# other issues
# get rid of is_remote
# is there an issue with our usage of a separate local directory as the remote

fsspec.filesystem(...)


from fsspec.spec import AbstractFileSystem

T = typing.TypeVar("T")


class A(AbstractFileSystem):
    def __init__(self, t: typing.Type):
        self._fs_cls = t

    def ls(self, path, detail=True, **kwargs):
        pass

    def cp_file(self, path1, path2, **kwargs):
        pass

    def _rm(self, path):
        pass

    def created(self, path):
        pass

    def modified(self, path):
        pass

    def sign(self, path, expiration=100, **kwargs):
        pass


def test_jklxji():
    remote_path = "s3://flyte-demo/datasets/sddemo/small.parquet"
    fap = FileAccessProvider(
        local_sandbox_dir=tempfile.mkdtemp(prefix="testnewfs"),
        raw_output_prefix="/Users/ytong/temp/data",
        data_config=Config.for_sandbox().data_config,
    )
    fap.get_data(remote_path, local_path="/Users/ytong/temp/")

    # if this works, then wrap it and use it.
    uri_fs, path = fs.FileSystem.from_uri(remote_path)
    wrapped_fs = ArrowFSWrapper(uri_fs)
    wrapped_fs.get()

    # if not then, call the base fsspec module and get the filesystem from there.
    fsspec.
    fsspec.filesystem
    # This is basically a lazily loaded AbstractFileSystem


