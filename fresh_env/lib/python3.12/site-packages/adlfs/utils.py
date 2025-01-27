from typing import Optional


def match_blob_version(blob, version_id: Optional[str]):
    blob_version_id = blob.get("version_id")
    return (
        version_id is None
        and (blob_version_id is None or blob.get("is_current_version"))
    ) or blob_version_id == version_id


async def filter_blobs(
    blobs,
    target_path,
    delimiter="/",
    version_id: Optional[str] = None,
    versions: bool = False,
):
    """
    Filters out blobs that do not come from target_path

    Parameters
    ----------
    blobs:  A list of candidate blobs to be returned from Azure

    target_path: Actual prefix of the blob folder

    delimiter: str
            Delimiter used to separate containers and files

    version_id: Spefic blob version ID to be returned
    """
    # remove delimiter and spaces, then add delimiter at the end
    target_path = target_path.strip(" " + delimiter) + delimiter
    finalblobs = [
        b
        for b in blobs
        if (
            b["name"].strip(" " + delimiter).startswith(target_path)
            and (versions or match_blob_version(b, version_id))
        )
    ]
    return finalblobs


async def get_blob_metadata(container_client, path, version_id: Optional[str] = None):
    async with container_client.get_blob_client(path) as bc:
        properties = await bc.get_blob_properties(version_id=version_id)
        if "metadata" in properties.keys():
            metadata = properties["metadata"]
        else:
            metadata = None
    return metadata


async def close_service_client(fs):
    """
    Implements asynchronous closure of service client for
    AzureBlobFile objects
    """
    await fs.service_client.close()


async def close_container_client(file_obj):
    """
    Implements asynchronous closure of container client for
    AzureBlobFile objects
    """
    await file_obj.container_client.close()


async def close_credential(file_obj):
    """
    Implements asynchronous closure of credentials for
    AzureBlobFile objects
    """
    if not isinstance(file_obj.credential, (type(None), str)):
        await file_obj.credential.close()
