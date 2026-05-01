"""
Module for post-processing validation and writing of Employee Records to storage.
"""

# pylint: disable=duplicate-code, trailing-newlines
import logging
import os
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


class EmployeeBackupValidator:  # pylint: disable=too-few-public-methods
    """Post-processing validation for employee records."""

    REQUIRED_DERIVED = [
        "salary_band",
        "seniority_level",
        "gender_normalized",
        "years_at_company",
        "is_senior",
    ]
    VALID_SALARY_BANDS = {"Entry", "Mid", "Senior", "Executive"}
    VALID_SENIORITY = {"Junior", "Mid-level", "Senior", "Principal", "Unknown"}

    def validate(self, df: pd.DataFrame) -> bool:
        """Runs checks to verify correctness of computed employee metrics."""
        logger.info("Starting backup validation.")
        passed = True

        missing = [c for c in self.REQUIRED_DERIVED if c not in df.columns]
        if missing:
            logger.error("Missing derived columns: %s", missing)
            return False

        invalid_band = ~df["salary_band"].isin(self.VALID_SALARY_BANDS)
        if invalid_band.any():
            logger.warning("salary_band: %s unexpected values.", invalid_band.sum())
            passed = False

        invalid_sen = ~df["seniority_level"].isin(self.VALID_SENIORITY)
        if invalid_sen.any():
            logger.warning("seniority_level: %s unexpected values.", invalid_sen.sum())
            passed = False

        if df["years_at_company"].dropna().lt(0).any():
            logger.warning("years_at_company has negative values.")
            passed = False

        if passed:
            logger.info("Backup validation PASSED.")
        else:
            logger.error("Backup validation FAILED.")
        return passed


class EmployeeWriter:  # pylint: disable=too-few-public-methods
    """Writes processed employee data to local folder and Azure Blob Storage."""

    def __init__(
        self,
        local_output_dir: str,
        azure_connection_str: str = None,
        azure_container: str = "employee-output",
    ):
        self.local_output_dir = local_output_dir
        self.azure_connection_str = azure_connection_str
        self.azure_container = azure_container

    def write(self, df: pd.DataFrame) -> str:
        """Writes transformed DataFrame to disk and cloud."""
        filename = f"employee_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        local_path = self._write_local(df, filename)
        self._write_azure(local_path, filename)
        return local_path

    def _write_local(self, df: pd.DataFrame, filename: str) -> str:
        os.makedirs(self.local_output_dir, exist_ok=True)
        output_path = os.path.join(self.local_output_dir, filename)
        df.to_csv(output_path, index=False)
        logger.info("Written locally to: %s", output_path)
        return output_path

    def _write_azure(self, local_path: str, filename: str) -> None:
        if not self.azure_connection_str:
            logger.warning("No Azure connection string — skipping Azure upload.")
            return
        try:
            from azure.storage.blob import (
                BlobServiceClient,
            )  # pylint: disable=import-outside-toplevel

            client = BlobServiceClient.from_connection_string(self.azure_connection_str)
            container = client.get_container_client(self.azure_container)
            if not container.exists():
                container.create_container()
            blob_name = f"part2/{filename}"
            with open(local_path, "rb") as f:
                container.upload_blob(name=blob_name, data=f, overwrite=True)
            logger.info("Uploaded to Azure: %s/%s", self.azure_container, blob_name)
        except ImportError:
            logger.error("Run: pip install azure-storage-blob")
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Azure upload failed: %s", e)
