import fsspec
from pyarrow import fs
from fsspec.implementations.arrow import ArrowFSWrapper


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
# need a class where


# other issues
# get rid of is_remote
# is there an issue with our usage of

class MyS3(object):
    def __new__(cls, *args, **kwargs):
        return ArrowFSWrapper(s3)


fsspec.register_implementation("s3", MyS3)
    
