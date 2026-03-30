def is_serverless_config(databricks_conf: dict) -> bool:
    """
    Detect if the Databricks configuration is for serverless compute.

    Serverless is indicated by having ``environment_key`` or ``environments``
    without any cluster config (``existing_cluster_id`` or ``new_cluster``).

    Args:
        databricks_conf (dict): The databricks job configuration dict.

    Returns:
        bool: True if the configuration targets serverless compute.
    """
    has_cluster_config = (
        databricks_conf.get("existing_cluster_id") is not None or databricks_conf.get("new_cluster") is not None
    )
    has_serverless_config = bool(databricks_conf.get("environment_key") or databricks_conf.get("environments"))
    return not has_cluster_config and has_serverless_config
