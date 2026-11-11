import json
from unittest import mock

import pytest
from asyncssh.sftp import SFTPNoSuchFile
from flytekitplugins.slurm.function.connector import SlurmFunctionConnector, SlurmJobMetadata

from flytekit.models.core.execution import TaskExecutionPhase


def _mock_conn(sftp_get):
    conn = mock.MagicMock()
    job = {"jobs": [{"job_state": ["COMPLETED"], "standard_output": "/home/ubuntu/out.log"}]}
    conn.run = mock.AsyncMock(return_value=mock.MagicMock(stdout=json.dumps(job)))
    sftp = mock.MagicMock()
    sftp.get = sftp_get
    conn.start_sftp_client.return_value.__aenter__ = mock.AsyncMock(return_value=sftp)
    conn.start_sftp_client.return_value.__aexit__ = mock.AsyncMock(return_value=False)
    return conn


@pytest.mark.asyncio
@pytest.mark.parametrize("stdout_exists", [True, False])
async def test_get_logs_job_stdout(stdout_exists):
    async def sftp_get(remote, local):
        if not stdout_exists:
            raise SFTPNoSuchFile("no such file")
        with open(local, "w") as f:
            f.write("hello from slurm")

    conn = _mock_conn(sftp_get)
    meta = SlurmJobMetadata(job_id="42", ssh_config={"host": "h"})
    with mock.patch("flytekitplugins.slurm.function.connector.get_ssh_conn", mock.AsyncMock(return_value=conn)):
        res = await SlurmFunctionConnector().get(meta)

    assert res.phase == TaskExecutionPhase.SUCCEEDED
    assert res.message == ("hello from slurm" if stdout_exists else "")
