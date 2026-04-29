import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)


class TaxiReader:
    """Reads the Yellow Taxi Trip Records parquet file."""

    SUPPORTED_EXTENSIONS = [".parquet"]

    def __init__(self, input_path: str):
        self.input_path = input_path

    def read(self) -> pd.DataFrame:
        _, ext = os.path.splitext(self.input_path)

        if ext.lower() not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: '{ext}'. Expected one of {self.SUPPORTED_EXTENSIONS}")

        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Input file not found: {self.input_path}")

        if os.path.getsize(self.input_path) == 0:
            raise ValueError(f"Input file is empty (0 bytes): {self.input_path}")

        logger.info(f"Reading file: {self.input_path}")
        try:
            df = pd.read_parquet(self.input_path)
        except Exception as e:
            raise ValueError(f"Failed to read parquet file '{self.input_path}': {e}") from e

        if df.empty:
            logger.warning("Input parquet file contains no rows.")

        logger.info(f"Read {len(df)} rows and {len(df.columns)} columns.")
        return df