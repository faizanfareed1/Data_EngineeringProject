import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)


class EmployeeReader:
    """Reads employee HR records from CSV or Excel files."""

    SUPPORTED_EXTENSIONS = [".csv", ".xlsx", ".xls"]

    def __init__(self, file_path: str):
        self.file_path = file_path

    def read(self) -> pd.DataFrame:
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        if os.path.getsize(self.file_path) == 0:
            raise ValueError(f"Input file is empty (0 bytes): {self.file_path}")

        _, ext = os.path.splitext(self.file_path.lower())
        if ext not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: '{ext}'. Supported: {self.SUPPORTED_EXTENSIONS}")

        logger.info(f"Reading file: {self.file_path}")

        try:
            if ext == ".csv":
                df = pd.read_csv(self.file_path)
            else:
                df = pd.read_excel(self.file_path)
        except Exception as e:
            raise ValueError(f"Failed to read file '{self.file_path}': {e}") from e

        if df.empty:
            logger.warning("File contains no data rows (headers only or empty).")

        logger.info(f"Read {len(df)} rows and {len(df.columns)} columns.")
        return df