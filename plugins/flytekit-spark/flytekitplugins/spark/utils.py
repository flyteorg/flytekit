def is_serverless_config(databricks_conf: dict) -> bool:
    """
    Detect if the Databricks configuration is for serverless compute.

    Serverless is indicated by having ``environment_key`` or ``environments``
    without any cluster config (``existing_cluster_id`` or ``new_cluster``).

    Args:
        databricks_conf: The databricks job configuration dict.

    Returns:
        True if the configuration targets serverless compute.
    """
    has_cluster_config = "existing_cluster_id" in databricks_conf or "new_cluster" in databricks_conf
    has_serverless_config = bool(databricks_conf.get("environment_key") or databricks_conf.get("environments"))
    return not has_cluster_config and has_serverless_config
