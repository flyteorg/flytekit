"""Helper functions for Lepton endpoint management.

This module provides utility functions for interacting with the lep CLI for endpoint operations.
Adapted from deployment_helpers.py patterns.
"""

import json
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional


def create_lepton_endpoint(spec: Dict[str, Any], endpoint_name: str) -> bool:
    """Create a Lepton endpoint using the lep CLI.

    Args:
        spec: The endpoint specification dictionary.
        endpoint_name: Name for the endpoint.

    Returns:
        True if endpoint creation succeeded, False otherwise.
    """
    # Write spec to temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(spec, f, indent=2)
        spec_file = f.name

    try:
        # Create endpoint using lep CLI
        result = subprocess.run(
            ["lep", "endpoint", "create", "--file", spec_file, "--name", endpoint_name],
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode == 0:
            print(f"Successfully created Lepton endpoint: {endpoint_name}")
            return True
        else:
            print(f"Failed to create Lepton endpoint: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        print(f"Timeout creating Lepton endpoint: {endpoint_name}")
        return False
    except subprocess.CalledProcessError as e:
        print(f"Error creating Lepton endpoint: {e}")
        return False
    finally:
        # Clean up temporary file
        Path(spec_file).unlink(missing_ok=True)


def delete_lepton_endpoint(endpoint_name: str) -> bool:
    """Delete a Lepton endpoint.

    Args:
        endpoint_name: Name of the endpoint to delete.

    Returns:
        True if deletion succeeded, False otherwise.
    """
    try:
        result = subprocess.run(
            ["lep", "endpoint", "remove", "--name", endpoint_name],
            capture_output=True,
            text=True,
            timeout=60,
        )

        return result.returncode == 0
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return False


def get_lepton_endpoint_status(endpoint_name: str) -> Optional[Dict[str, Any]]:
    """Get the status of a Lepton endpoint.

    Args:
        endpoint_name: Name of the endpoint.

    Returns:
        Endpoint status info if endpoint exists, None otherwise.
    """
    try:
        result = subprocess.run(
            ["lep", "endpoint", "get", "--name", endpoint_name],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            return json.loads(result.stdout)
        else:
            return None
    except (
        subprocess.TimeoutExpired,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
    ):
        return None


def wait_for_lepton_endpoint_ready(endpoint_name: str, timeout: int = 600) -> bool:
    """Wait for a Lepton endpoint to become ready.

    Args:
        endpoint_name: Name of the endpoint.
        timeout: Maximum time to wait in seconds.

    Returns:
        True if endpoint becomes ready, False if timeout.
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        endpoint_info = get_lepton_endpoint_status(endpoint_name)

        if not endpoint_info:
            print(f"Endpoint {endpoint_name} not found, waiting...")
            time.sleep(10)
            continue

        status = endpoint_info.get("status", {})

        if isinstance(status, dict):
            state = status.get("state", "").lower()
            if state == "ready":
                print(f"Endpoint {endpoint_name} is ready!")
                return True
            elif state in ["failed", "error"]:
                print(f"Lepton endpoint {endpoint_name} failed to start")
                return False
        elif isinstance(status, str) and status.lower() == "ready":
            print(f"Endpoint {endpoint_name} is ready!")
            return True
        elif isinstance(status, str) and status.lower() in ["failed", "error"]:
            print(f"Lepton endpoint {endpoint_name} failed to start")
            return False

        print(f"Waiting for endpoint {endpoint_name} to be ready (status: {status})")
        time.sleep(10)

    print(f"Timeout waiting for endpoint {endpoint_name} to be ready")
    return False


def get_lepton_endpoint_url(endpoint_name: str) -> Optional[str]:
    """Get the URL of a Lepton endpoint.

    Args:
        endpoint_name: Name of the endpoint.

    Returns:
        Endpoint URL if available, None otherwise.
    """
    endpoint_info = get_lepton_endpoint_status(endpoint_name)

    if not endpoint_info:
        return None

    status = endpoint_info.get("status", {})
    if isinstance(status, dict):
        endpoint_data = status.get("endpoint", {})
        if isinstance(endpoint_data, dict):
            return endpoint_data.get("external_endpoint")

    return None


def is_lep_cli_authenticated() -> bool:
    """Check if lep CLI is authenticated.

    Returns:
        True if authenticated, False otherwise.
    """
    try:
        result = subprocess.run(
            ["lep", "workspace", "get"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
        return False


def authenticate_lep_cli(workspace_id: str, token: str) -> bool:
    """Authenticate lep CLI with workspace credentials.

    Args:
        workspace_id: Lepton workspace ID.
        token: Lepton API token.

    Returns:
        True if authentication succeeded, False otherwise.
    """
    try:
        result = subprocess.run(
            ["lep", "login", "--workspace-id", workspace_id, "--token", token],
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
        return False
