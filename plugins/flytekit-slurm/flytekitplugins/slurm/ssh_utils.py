"""
Utilities of asyncssh connections.
"""

import sys
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, Union
from flytekit import logger
import asyncssh
from asyncssh import SSHClientConnection
from asyncssh.known_hosts import KnownHostsArg

from flytekit.extend.backend.utils import get_agent_secret

T = TypeVar("T", bound="SSHConfig")
SLURM_PRIVATE_KEY = "FLYTE_SLURM_PRIVATE_KEY"


@dataclass(frozen=True)
class SSHConfig:
    """A customized version of SSHClientConnectionOptions, tailored to specific needs.

    This config is based on the official SSHClientConnectionOptions but includes
    only a subset of options, with some fields adjusted to be optional or required.
    For the official options, please refer to:
    https://asyncssh.readthedocs.io/en/latest/api.html#asyncssh.SSHClientConnectionOptions

    Args:
        host: The hostname or address to connect to.
        username: The username to authenticate as on the server.
        client_keys: File paths to private keys which will be used to authenticate the
            client via public key authentication. The default value is not None since
            client public key authentication is mandatory.
        known_hosts: The list of keys which will be used to validate the server host key
            presented during the SSH handshake. If this is not specified, the keys will
            be looked up in the file .ssh/known_hosts. If this is explicitly set to None,
            server host key validation will be disabled.
    """

    host: str
    username: str
    client_keys: Union[str, List[str], Tuple[str, ...]] = ()
    known_hosts: Optional[KnownHostsArg] = None

    @classmethod
    def from_dict(cls: Type[T], ssh_config: Dict[str, Any]) -> T:
        return cls(**ssh_config)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


async def ssh_connect(ssh_config: Dict[str, Any]) -> SSHClientConnection:
    """Make an SSH client connection.

    Args:
        ssh_config: Options of SSH client connection defined in SSHConfig.

    Returns:
        An SSH client connection object.
    """
    # Validate ssh_config
    ssh_config = SSHConfig.from_dict(ssh_config).to_dict()

    # Make the first SSH connection using either OpenSSH client config files or
    # a user-defined private key. If using OpenSSH config, it will attempt to
    # load settings from ~/.ssh/config.
    try:
        conn = await asyncssh.connect(**ssh_config)
        return conn
    except Exception as e:
        logger.info(
            "Failed to make an SSH connection using the default OpenSSH client config (~/.ssh/config) or "
            f"the provided private keys. Error details:\n{e}"
        )

    try:
        default_client_key = get_agent_secret(secret_key=SLURM_PRIVATE_KEY)
    except ValueError:
        logger.info("The secret for key FLYTE_SLURM_PRIVATE_KEY is not set.")
        default_client_key = None

    if default_client_key is None and ssh_config.get("client_keys") == ():
        raise ValueError(
            "Both the secret for key FLYTE_SLURM_PRIVATE_KEY and ssh_config['private_key'] are missing. "
            "At least one must be set."
        )

    import os

    # ABAO VERSION
    # Construct a list of file paths to private keys
    # client_keys = []
    # if default_client_key is not None:
    #     # Write the private key to a local path
    #     # This may not be a good practice...
    #     private_key_path = os.path.abspath("./slurm_private_key")
    #
    #     with open("./slurm_private_key", "w") as f:
    #         f.write(default_client_key)
    #     client_keys.append("./slurm_private_key")

    # HANRU VERSION
    client_keys = []
    if default_client_key is not None:
        # Write the private key to a local path
        # This may not be a good practice...
        private_key_path = os.path.abspath("./slurm_private_key")
        with open(private_key_path, "w") as f:
            f.write(default_client_key)
        client_keys.append(private_key_path)

    user_client_keys = ssh_config.get("client_keys")
    if user_client_keys is not None:
        client_keys.extend([user_client_keys] if isinstance(user_client_keys, str) else user_client_keys)

    ssh_config["client_keys"] = client_keys
    logger.info(f"Updated SSH config: {ssh_config}")
    try:
        conn = await asyncssh.connect(**ssh_config)
        return conn
    except Exception as e:
        logger.info(
            "Failed to make an SSH connection using the provided private keys. Please verify your setup."
            f"Error details:\n{e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    import asyncio

    async def test_connect():
        conn = await ssh_connect({"host": "aws2", "username": "ubuntu"})
        res = await conn.run("echo [TEST] SSH connection", check=True)
        out = res.stdout

        return out

    logger.info(asyncio.run(test_connect()))
