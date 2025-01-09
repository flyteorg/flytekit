"""
Common utilities for flyte remote runs in integration tests.
"""
import os
import json
import tempfile
import pathlib

import botocore.session
from botocore.client import BaseClient
from flytekit.configuration import Config
from flytekit.remote.remote import FlyteRemote


# Define constants
CONFIG = os.environ.get("FLYTECTL_CONFIG", str(pathlib.Path.home() / ".flyte" / "config-sandbox.yaml"))
PROJECT = "flytesnacks"
DOMAIN = "development"


class SimpleFileTransfer:
    """Utilities for file transfer to minio s3 bucket.

    Mainly support single file uploading and automatic teardown.
    """

    def __init__(self) -> None:
        self._remote = FlyteRemote(
            config=Config.auto(config_file=CONFIG),
            default_project=PROJECT,
            default_domain=DOMAIN
        )
        self._s3_client = self._get_minio_s3_client(self._remote)

    def _get_minio_s3_client(self, remote: FlyteRemote) -> BaseClient:
        """Creat a botocore client."""
        minio_s3_config = remote.file_access.data_config.s3
        sess = botocore.session.get_session()

        return sess.create_client(
            "s3",
            endpoint_url=minio_s3_config.endpoint,
            aws_access_key_id=minio_s3_config.access_key_id,
            aws_secret_access_key=minio_s3_config.secret_access_key,
        )

    def upload_file(self, file_type: str) -> str:
        """Upload a single file to minio s3 bucket.

        Args:
            file_type: File type. Support "txt" and "json".

        Returns:
            remote_file_path: Remote file path.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_file_path = self._dump_tmp_file(file_type, tmp_dir)

            # Upload to minio s3 bucket
            _, remote_file_path = self._remote.upload_file(
                to_upload=local_file_path,
                project=PROJECT,
                domain=DOMAIN,
            )

        return remote_file_path

    def _dump_tmp_file(self, file_type: str, tmp_dir: str) -> str:
        """Generate and dump a temporary file locally.

        Args:
            file_type: File type.
            tmp_dir: Temporary directory.

        Returns:
            tmp_file_path: Temporary local file path.
        """
        if file_type == "txt":
            tmp_file_path = pathlib.Path(tmp_dir) / "test.txt"
            with open(tmp_file_path, "w") as f:
                f.write("Hello World!")
        elif file_type == "json":
            d = {"name": "john", "height": 190}
            tmp_file_path = pathlib.Path(tmp_dir) / "test.json"
            with open(tmp_file_path, "w") as f:
                json.dump(d, f)
        elif file_type == "parquet":
            # Because `upload_file` accepts a single file only, we specify 00000 to make it a single file
            tmp_file_path = pathlib.Path(__file__).parent / "workflows/basic/data/df.parquet/00000"

        return tmp_file_path

    def delete_file(self, bucket: str, key: str) -> None:
        """Delete the remote file from minio s3 bucket to free the space.

        Args:
            bucket: s3 bucket name.
            key: Key name of the object.
        """
        res = self._s3_client.delete_object(Bucket=bucket, Key=key)
        assert res["ResponseMetadata"]["HTTPStatusCode"] == 204
