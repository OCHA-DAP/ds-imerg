import os

from azure.storage.blob import ContainerClient

DEV_BLOB_SAS = os.getenv("DEV_BLOB_SAS")
DEV_BLOB_NAME = "imb0chd0dev"
DEV_BLOB_BASE_URL = f"https://{DEV_BLOB_NAME}.blob.core.windows.net/"

GLOBAL_CONTAINER_NAME = "global"
DEV_BLOB_GLB_URL = (
    DEV_BLOB_BASE_URL + GLOBAL_CONTAINER_NAME + "?" + DEV_BLOB_SAS
)


def get_glb_container_client() -> ContainerClient:
    """
    Get the container client for the global container
    Returns
    -------

    """
    return ContainerClient.from_container_url(DEV_BLOB_GLB_URL)
