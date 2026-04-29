import pandas as pd
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class TaxiWriter:
    """
    Writes the processed DataFrame to:
    1. A local output folder (Parquet)
    2. Azure Blob Storage (Parquet)
    """

    def __init__(self, local_output_dir: str, azure_connection_str: str = None,
                 azure_container: str = "taxi-output"):
        self.local_output_dir = local_output_dir
        self.azure_connection_str = azure_connection_str
        self.azure_container = azure_container

    def write(self, df: pd.DataFrame) -> str:
        filename = f"yellow_taxi_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"

        local_path = self._write_local(df, filename)
        self._write_azure(local_path, filename)

        return local_path

    def _write_local(self, df: pd.DataFrame, filename: str) -> str:
        os.makedirs(self.local_output_dir, exist_ok=True)
        output_path = os.path.join(self.local_output_dir, filename)
        df.to_parquet(output_path, index=False)
        logger.info(f"Written locally to: {output_path}")
        return output_path

    def _write_azure(self, local_path: str, filename: str) -> None:
        if not self.azure_connection_str:
            logger.warning("No Azure connection string provided — skipping Azure upload.")
            return

        try:
            from azure.storage.blob import BlobServiceClient

            blob_service_client = BlobServiceClient.from_connection_string(self.azure_connection_str)
            container_client = blob_service_client.get_container_client(self.azure_container)

            # Create container if it doesn't exist
            if not container_client.exists():
                container_client.create_container()
                logger.info(f"Created Azure container: {self.azure_container}")

            blob_name = f"part1/{filename}"
            with open(local_path, "rb") as data:
                container_client.upload_blob(name=blob_name, data=data, overwrite=True)

            logger.info(f"Uploaded to Azure Blob Storage: {self.azure_container}/{blob_name}")

        except ImportError:
            logger.error("azure-storage-blob not installed. Run: pip install azure-storage-blob")
        except Exception as e:
            logger.error(f"Azure upload failed: {e}")