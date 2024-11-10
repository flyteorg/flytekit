from utils.constants import EI, EI_DEV, EI_DEV2, EI_DEV3, GRID1, GRID1_PROD, GRID2, PROD_LTX1


job_type_to_created_by = {
    "mpi": "MPIJob",
    "pytorch": "PyTorchJob",
    "raycluster": "RayCluster",
    "tf": "TFJob",
}


def union_maps(*maps: dict) -> dict:
    composite = {}
    for m in maps:
        if m:  # Check if the map (dictionary) is not None or empty
            composite.update(m)  # Update the composite map with the contents of the current map
    return composite


def parse_kkid_from_release_name(name: str) -> str:
    parts = name.split('-')
    if len(parts) == 3:
        return parts[-1]
    else:
        raise ValueError("Invalid name format. Expected 'gnn-hashcode1-hashcode2' format.")


# TODO (shuliang) replace this with config map set up because currently no error handling
# and does not deal with different product tag
def get_kingkong_endpoint(cluster: str):
    mapping = {
        EI: "https://kingkong-server.ei-ltx1-k8s0.stg.linkedin.com/api/v1/execution",
        EI_DEV: "https://kingkong-server.ei-ltx1-k8s0.stg.linkedin.com/api/v1/execution",
        EI_DEV2: "https://kingkong-server.ei-ltx1-k8s0.stg.linkedin.com/api/v1/execution",
        EI_DEV3: "https://kingkong-server.ei-ltx1-k8s0.stg.linkedin.com/api/v1/execution",
        GRID1: "https://kingkong-server.grid1-k8s0.grid.linkedin.com/api/v1/execution",
        GRID1_PROD: "https://kingkong-server.grid1-k8s0.grid.linkedin.com/api/v1/execution",
        GRID2: "https://kingkong-server.grid2-k8s0.grid.linkedin.com/api/v1/execution",
        PROD_LTX1: "https://2.kingkong-server.prod-ltx1.atd.disco.linkedin.com:30126/api/v1/execution"
    }
    cluster = cluster.lower()
    if cluster not in mapping.keys():
        raise Exception(f"{cluster} is not valid or supported yet. The supported clusters are ei-dev2, grid1, grid-prod, grid2, prod-ltx1")
    return mapping[cluster]
