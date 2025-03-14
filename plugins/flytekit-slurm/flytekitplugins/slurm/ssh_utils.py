"""
Utilities of asyncssh connections.
"""

import os
import sys
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, Union

import asyncssh
from asyncssh import SSHClientConnection

from flytekit import logger
from flytekit.extend.backend.utils import get_connector_secret

T = TypeVar("T", bound="SSHConfig")
SLURM_PRIVATE_KEY = "FLYTE_SLURM_PRIVATE_KEY"


@dataclass
class SlurmCluster:
    """A Slurm cluster instance is defined by a pair of (Slurm host, username).

    Attributes:
        host (str): The hostname or address to connect to.
        username (Optional[str]): The username to authenticate as on the server.
    """

    host: str
    username: Optional[str] = None

    def __hash__(self):
        return hash((self.host, self.username))


@dataclass(frozen=True)
class SSHConfig:
    """A customized version of SSHClientConnectionOptions, tailored to specific needs.

    This config is based on the official SSHClientConnectionOptions but includes
    only a subset of options, with some fields adjusted to be optional or required.
    For the official options, please refer to:
    https://asyncssh.readthedocs.io/en/latest/api.html#asyncssh.SSHClientConnectionOptions

    Attributes:
        host (str): The hostname or address to connect to.
        username (Optional[str]): The username to authenticate as on the server.
        client_keys (Union[str, List[str], Tuple[str, ...]]): File paths to private keys which will be used to authenticate the
            client via public key authentication. The default value is an empty tuple since
            client public key authentication is mandatory.
    """

    host: str
    username: Optional[str] = None
    client_keys: Union[str, List[str], Tuple[str, ...]] = ()

    @classmethod
    def from_dict(cls: Type[T], ssh_config: Dict[str, Any]) -> T:
        return cls(**ssh_config)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def __eq__(self, other):
        if not isinstance(other, SSHConfig):
            return False
        return self.host == other.host and self.username == other.username and self.client_keys == other.client_keys


async def ssh_connect(ssh_config: Dict[str, Any]) -> SSHClientConnection:
    """Make an SSH client connection.

    Args:
        ssh_config (Dict[str, Any]): Options of SSH client connection defined in SSHConfig.

    Returns:
        SSHClientConnection: An SSH client connection object.

    Raises:
        ValueError: If both FLYTE_SLURM_PRIVATE_KEY secret and ssh_config['private_key'] are missing.
    """
    # Validate ssh_config
    ssh_config = SSHConfig.from_dict(ssh_config).to_dict()
    # This is required to avoid the error "asyncssh.misc.HostKeyNotVerifiable" when connecting to a new host.
    ssh_config["known_hosts"] = None

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
        default_client_key = get_connector_secret(secret_key=SLURM_PRIVATE_KEY)
    except ValueError:
        logger.info("The secret for key FLYTE_SLURM_PRIVATE_KEY is not set.")
        default_client_key = None

    if default_client_key is None and ssh_config.get("client_keys") == ():
        raise ValueError(
            "Both the secret for key FLYTE_SLURM_PRIVATE_KEY and ssh_config['private_key'] are missing. "
            "At least one must be set."
        )

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


async def get_ssh_conn(
    ssh_config: Dict[str, Union[str, List[str], Tuple[str, ...]]],
    slurm_cluster_to_ssh_conn: Dict[SlurmCluster, SSHClientConnection],
) -> Tuple[SlurmCluster, SSHClientConnection]:
    """
    Get an existing SSH connection or create a new one if needed.

    Args:
        ssh_config (Dict[str, Union[str, List[str], Tuple[str, ...]]]):
            SSH configuration dictionary, including host and username.
        slurm_cluster_to_ssh_conn (Dict[SlurmCluster, SSHClientConnection]):
            A mapping of SlurmCluster to existing SSHClientConnection objects.

    Returns:
        Tuple[SlurmCluster, SSHClientConnection]:
            A tuple containing (SlurmCluster, SSHClientConnection). If no connection
            for the given SlurmCluster exists, a new one is created and cached.
    """

    # (Optional) normal code comment instead of docstring line:
    # Is it necessary to ensure immutability in this function?

    host = ssh_config.get("host")
    username = ssh_config.get("username")
    slurm_cluster = SlurmCluster(host=host, username=username)

    if slurm_cluster_to_ssh_conn.get(slurm_cluster) is None:
        logger.info("SSH connection key not found, creating new connection")
        conn = await ssh_connect(ssh_config=ssh_config)
        slurm_cluster_to_ssh_conn[slurm_cluster] = conn
    else:
        conn = slurm_cluster_to_ssh_conn[slurm_cluster]
        try:
            await conn.run("echo [TEST] SSH connection", check=True)
            logger.info("Re-using new connection")
        except Exception as e:
            logger.info(f"Re-establishing SSH connection due to error: {e}")
            conn = await ssh_connect(ssh_config=ssh_config)
            slurm_cluster_to_ssh_conn[slurm_cluster] = conn

    return conn


if __name__ == "__main__":
    import asyncio

    async def test_connect():
        conn = await ssh_connect({"host": "aws2", "username": "ubuntu"})
        res = await conn.run("echo [TEST] SSH connection", check=True)
        out = res.stdout

        return out

    logger.info(asyncio.run(test_connect()))
