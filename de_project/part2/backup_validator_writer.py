import pandas as pd
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class EmployeeBackupValidator:
    """Post-processing validation for employee records."""

    REQUIRED_DERIVED = ["salary_band", "seniority_level", "gender_normalized",
                        "years_at_company", "is_senior"]
    VALID_SALARY_BANDS = {"Entry", "Mid", "Senior", "Executive"}
    VALID_SENIORITY = {"Junior", "Mid-level", "Senior", "Principal", "Unknown"}

    def validate(self, df: pd.DataFrame) -> bool:
        logger.info("Starting backup validation.")
        passed = True

        missing = [c for c in self.REQUIRED_DERIVED if c not in df.columns]
        if missing:
            logger.error(f"Missing derived columns: {missing}")
            return False

        invalid_band = ~df["salary_band"].isin(self.VALID_SALARY_BANDS)
        if invalid_band.any():
            logger.warning(f"salary_band: {invalid_band.sum()} unexpected values.")
            passed = False

        invalid_sen = ~df["seniority_level"].isin(self.VALID_SENIORITY)
        if invalid_sen.any():
            logger.warning(f"seniority_level: {invalid_sen.sum()} unexpected values.")
            passed = False

        if df["years_at_company"].dropna().lt(0).any():
            logger.warning("years_at_company has negative values.")
            passed = False

        if passed:
            logger.info("Backup validation PASSED.")
        else:
            logger.error("Backup validation FAILED.")
        return passed


class EmployeeWriter:
    """Writes processed employee data to local folder and Azure Blob Storage."""

    def __init__(self, local_output_dir: str, azure_connection_str: str = None,
                 azure_container: str = "employee-output"):
        self.local_output_dir = local_output_dir
        self.azure_connection_str = azure_connection_str
        self.azure_container = azure_container

    def write(self, df: pd.DataFrame) -> str:
        filename = f"employee_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        local_path = self._write_local(df, filename)
        self._write_azure(local_path, filename)
        return local_path

    def _write_local(self, df: pd.DataFrame, filename: str) -> str:
        os.makedirs(self.local_output_dir, exist_ok=True)
        output_path = os.path.join(self.local_output_dir, filename)
        df.to_csv(output_path, index=False)
        logger.info(f"Written locally to: {output_path}")
        return output_path

    def _write_azure(self, local_path: str, filename: str) -> None:
        if not self.azure_connection_str:
            logger.warning("No Azure connection string — skipping Azure upload.")
            return
        try:
            from azure.storage.blob import BlobServiceClient
            client = BlobServiceClient.from_connection_string(self.azure_connection_str)
            container = client.get_container_client(self.azure_container)
            if not container.exists():
                container.create_container()
            blob_name = f"part2/{filename}"
            with open(local_path, "rb") as f:
                container.upload_blob(name=blob_name, data=f, overwrite=True)
            logger.info(f"Uploaded to Azure: {self.azure_container}/{blob_name}")
        except ImportError:
            logger.error("Run: pip install azure-storage-blob")
        except Exception as e:
            logger.error(f"Azure upload failed: {e}")
