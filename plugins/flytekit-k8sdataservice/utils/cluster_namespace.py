from .constants import EI, EI_DEV, EI_DEV2, EI_DEV3, GRID1, GRID1_PROD, GRID2, PROD_LTX1


cluster_namespace_mapping = {
        EI: "kk-flyte",
        EI_DEV: "kk-flyte-dev",
        # we uses ei-dev2
        EI_DEV2: "kk-flyte-dev2",
        EI_DEV3: "kk-flyte-dev3",
        GRID1: "kingkong-dev",
        GRID1_PROD: "kk-flyte-prod",
        # Special note: grid2 should be using kk-flyte-prod for a production dataplane namespace as other clusters.
        # But the flyte team has set up this way to use kingkong developing namespace and no priority to change it.
        GRID2: "kingkong-dev",
        PROD_LTX1: "kk-flyte-prod"
    }


def get_execution_namespace(cluster: str):
    cluster = cluster.lower()
    if cluster not in cluster_namespace_mapping.keys():
        raise Exception(f"{cluster} is not valid or supported yet. The supported clusters are ei-dev2, grid1, grid-prod, grid2, prod-ltx1")
    return cluster_namespace_mapping[cluster]
