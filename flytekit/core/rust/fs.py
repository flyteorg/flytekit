import asyncio
from s3fs import S3FileSystem
from fsspec.callbacks import _DEFAULT_CALLBACK
import rustfs

class RustS3FileSystem(S3FileSystem):
    """
    Want this to behave mostly just like the HTTP file system.
    """

    def __init__(self, **s3kwargs):
        super().__init__(**s3kwargs)
        self.s = rustfs.S3FileSystem(endpoint=s3kwargs['client_kwargs']['endpoint_url'])

    async def _put_file(self, lpath, rpath, callback=_DEFAULT_CALLBACK, **kwargs):
        #TODO
        bucket, key, _ = self.split_path(rpath)
        await asyncio.to_thread(self.s.put_file, lpath, bucket, key)
        #TODO

    async def _get_file(self, lpath, rpath, callback=_DEFAULT_CALLBACK, **kwargs):
        #TODO
        bucket, key, _ = self.split_path(rpath)
        await asyncio.to_thread(self.s.get_file, lpath, bucket, key)
        #TODO
    
