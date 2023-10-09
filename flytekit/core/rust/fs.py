import asyncio
import mimetypes
import os
import socket
from s3fs import S3FileSystem
from fsspec.callbacks import _DEFAULT_CALLBACK
import rustfs
from botocore.exceptions import ClientError, HTTPClientError, ParamValidationError
from botocore.parsers import ResponseParserError
from urllib3.exceptions import IncompleteRead
from fsspec.asyn import (
    AsyncFileSystem,
    AbstractAsyncStreamedFile,
    sync,
    sync_wrapper,
    FSTimeoutError,
    _run_coros_in_chunks,
)
import aiofiles

def version_id_kw(version_id):
    """Helper to make versionId kwargs.

    Not all boto3 methods accept a None / empty versionId so dictionary expansion solves
    that problem.
    """
    if version_id:
        return {"VersionId": version_id}
    else:
        return {}
    
S3_RETRYABLE_ERRORS = (
    socket.timeout,
    HTTPClientError,
    IncompleteRead,
    FSTimeoutError,
    ResponseParserError,
)

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
    
class PythonS3FileSystem(S3FileSystem):
    def __init__(self, **s3kwargs):
        super().__init__(**s3kwargs)

    async def _put_file(
        self, lpath, rpath, callback=_DEFAULT_CALLBACK, chunksize=50 * 2**20, **kwargs
    ):
        bucket, key, _ = self.split_path(rpath)
        if os.path.isdir(lpath):
            if key:
                # don't make remote "directory"
                return
            else:
                await self._mkdir(lpath)
        size = os.path.getsize(lpath)
        callback.set_size(size)

        if "ContentType" not in kwargs:
            content_type, _ = mimetypes.guess_type(lpath)
            if content_type is not None:
                kwargs["ContentType"] = content_type

        with open(lpath, "rb") as f0:
            if size < min(5 * 2**30, 2 * chunksize):
                chunk = f0.read()
                await self._call_s3(
                    "put_object", Bucket=bucket, Key=key, Body=chunk, **kwargs
                )
                callback.relative_update(size)
            else:
                mpu = await self._call_s3(
                    "create_multipart_upload", Bucket=bucket, Key=key, **kwargs
                )

                async def upload_chunk(chunk, part_number):
                    result = await self._call_s3(
                        "upload_part",
                        Bucket=bucket,
                        PartNumber=part_number,
                        UploadId=mpu["UploadId"],
                        Body=chunk,
                        Key=key,
                    )
                    return {"PartNumber": part_number, "ETag": result["ETag"]}
                tasks = []
                while True:
                    chunk = f0.read(chunksize)
                    if not chunk:
                        break
                    tasks.append(
                        upload_chunk(chunk, len(tasks) + 1)
                    )
                    callback.relative_update(len(chunk))

                parts = await asyncio.gather(*tasks)
                await self._call_s3(
                    "complete_multipart_upload",
                    Bucket=bucket,
                    Key=key,
                    UploadId=mpu["UploadId"],
                    MultipartUpload={"Parts": parts},
                )
        while rpath:
            self.invalidate_cache(rpath)
            rpath = self._parent(rpath)

    async def _get_file(
        self, rpath, lpath, callback=_DEFAULT_CALLBACK, version_id=None
    ):
        if os.path.isdir(lpath):
            return
        file_info = await self._info(path=rpath, version_id=version_id)
        file_size = file_info["size"]
        bucket, key, vers = self.split_path(rpath)
        chunksize = 50 * 2**20
        if file_size is None or file_size < 2 * chunksize:
            async def _open_file(range: int):
                kw = self.req_kw.copy()
                if range:
                    kw["Range"] = f"bytes={range}-"
                resp = await self._call_s3(
                    "get_object",
                    Bucket=bucket,
                    Key=key,
                    **version_id_kw(version_id or vers),
                    **self.req_kw,
                )
                return resp["Body"], resp.get("ContentLength", None)

            body, content_length = await _open_file(range=0)
            callback.set_size(content_length)

            failed_reads = 0
            bytes_read = 0

            try:
                with open(lpath, "wb") as f0:
                    while True:
                        try:
                            chunk = await body.read(2**16)
                        except S3_RETRYABLE_ERRORS:
                            failed_reads += 1
                            if failed_reads >= self.retries:
                                # Give up if we've failed too many times.
                                raise
                            # Closing the body may result in an exception if we've failed to read from it.
                            try:
                                body.close()
                            except Exception:
                                pass

                            await asyncio.sleep(min(1.7**failed_reads * 0.1, 15))
                            # Byte ranges are inclusive, which means we need to be careful to not read the same data twice
                            # in a failure.
                            # Examples:
                            # Read 1 byte -> failure, retry with read_range=0, byte range should be 0-
                            # Read 1 byte, success. Read 1 byte: failure. Retry with read_range=2, byte-range should be 2-
                            # Read 1 bytes, success. Read 1 bytes: success. Read 1 byte, failure. Retry with read_range=3,
                            # byte-range should be 3-.
                            body, _ = await _open_file(bytes_read + 1)
                            continue

                        if not chunk:
                            break
                        bytes_read += len(chunk)
                        segment_len = f0.write(chunk)
                        callback.relative_update(segment_len)
            finally:
                try:
                    body.close()
                except Exception:
                    pass
        else:
            with open(lpath, "wb") as f0:
                async def download_chunk(chunk_index: int):
                    kw = self.req_kw.copy()
                    start_byte = chunk_index * chunksize
                    end_byte = min(start_byte + chunksize, file_size) - 1
                    kw["Range"] = f"bytes={start_byte}-{end_byte}"
                    resp = await self._call_s3(
                        "get_object",
                        Bucket=bucket,
                        Key=key,
                        **version_id_kw(version_id or vers),
                        **kw,
                    )
                    body = resp["Body"]
                    failed_reads = 0
                    bytes_read = 0
                    try:
                        while True:
                            try:
                                chunk = await body.read(2**16)
                            except S3_RETRYABLE_ERRORS:
                                print("err")
                                failed_reads += 1
                                if failed_reads >= self.retries:
                                    # Give up if we've failed too many times.
                                    raise
                                # Closing the body may result in an exception if we've failed to read from it.
                                try:
                                    body.close()
                                except Exception:
                                    pass

                                await asyncio.sleep(min(1.7**failed_reads * 0.1, 15))
                                body, _ = await _open_file(bytes_read + 1)
                                continue

                            if not chunk:
                                break
                            f0.seek(start_byte + bytes_read)
                            segment_len = f0.write(chunk)
                            bytes_read += len(chunk)
                            callback.relative_update(segment_len)
                    finally:
                        try:
                            body.close()
                        except Exception:
                            pass

                chunk_count = file_size // chunksize
                if file_size % chunksize > 0:
                    chunk_count += 1
                tasks = []
                for i in range(chunk_count):
                    tasks.append(download_chunk(i))
                await asyncio.gather(*tasks)
    