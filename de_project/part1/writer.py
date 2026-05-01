"""
Module for writing the processed Yellow Taxi DataFrame to local and cloud storage.
"""

# pylint: disable=duplicate-code, trailing-newlines
import logging
import os
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


class TaxiWriter:  # pylint: disable=too-few-public-methods
    """
    Writes the processed DataFrame to:
    1. A local output folder (Parquet)
    2. Azure Blob Storage (Parquet)
    """

    def __init__(
        self,
        local_output_dir: str,
        azure_connection_str: str = None,
        azure_container: str = "taxi-output",
    ):
        self.local_output_dir = local_output_dir
        self.azure_connection_str = azure_connection_str
        self.azure_container = azure_container

    def write(self, df: pd.DataFrame) -> str:
        """Writes the transformed DataFrame to local disk and cloud."""
        filename = (
            f"yellow_taxi_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        )

        local_path = self._write_local(df, filename)
        self._write_azure(local_path, filename)

        return local_path

    def _write_local(self, df: pd.DataFrame, filename: str) -> str:
        os.makedirs(self.local_output_dir, exist_ok=True)
        output_path = os.path.join(self.local_output_dir, filename)
        df.to_parquet(output_path, index=False)
        logger.info("Written locally to: %s", output_path)
        return output_path

    def _write_azure(self, local_path: str, filename: str) -> None:
        if not self.azure_connection_str:
            logger.warning(
                "No Azure connection string provided — skipping Azure upload."
            )
            return

        try:
            from azure.storage.blob import (
                BlobServiceClient,
            )  # pylint: disable=import-outside-toplevel

            blob_service_client = BlobServiceClient.from_connection_string(
                self.azure_connection_str
            )
            container_client = blob_service_client.get_container_client(
                self.azure_container
            )

            # Create container if it doesn't exist
            if not container_client.exists():
                container_client.create_container()
                logger.info("Created Azure container: %s", self.azure_container)

            blob_name = f"part1/{filename}"
            with open(local_path, "rb") as data:
                container_client.upload_blob(name=blob_name, data=data, overwrite=True)

            logger.info(
                "Uploaded to Azure Blob Storage: %s/%s", self.azure_container, blob_name
            )

        except ImportError:
            logger.error(
                "azure-storage-blob not installed. Run: pip install azure-storage-blob"
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Azure upload failed: %s", e)
