"""
Module for reading NYC Yellow Taxi records from parquet files.
"""

# pylint: disable=duplicate-code, trailing-newlines
import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


class TaxiReader:  # pylint: disable=too-few-public-methods
    """Reads the Yellow Taxi Trip Records parquet file."""

    SUPPORTED_EXTENSIONS = [".parquet"]

    def __init__(self, input_path: str):
        self.input_path = input_path

    def read(self) -> pd.DataFrame:
        """Loads Yellow Taxi data from local filesystem path."""
        _, ext = os.path.splitext(self.input_path)

        if ext.lower() not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported file type: '{ext}'. Expected one of {self.SUPPORTED_EXTENSIONS}"
            )

        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Input file not found: {self.input_path}")

        if os.path.getsize(self.input_path) == 0:
            raise ValueError(f"Input file is empty (0 bytes): {self.input_path}")

        logger.info("Reading file: %s", self.input_path)
        try:
            df = pd.read_parquet(self.input_path)
        except Exception as e:
            raise ValueError(
                f"Failed to read parquet file '{self.input_path}': {e}"
            ) from e

        if df.empty:
            logger.warning("Input parquet file contains no rows.")

        logger.info("Read %s rows and %s columns.", len(df), len(df.columns))
        return df
