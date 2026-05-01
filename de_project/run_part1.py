"""
Script to run Part 1: Yellow Taxi Batch Pipeline.
"""

import logging
import os
import sys
from pathlib import Path

from backup_validator import TaxiBackupValidator  # pylint: disable=import-error
from dotenv import load_dotenv
from processor import TaxiProcessor  # pylint: disable=import-error
from reader import TaxiReader  # pylint: disable=import-error
from validator import TaxiValidator  # pylint: disable=import-error
from writer import TaxiWriter  # pylint: disable=import-error

load_dotenv(Path(__file__).parent / "airflow-docker" / ".env")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

sys.path.insert(0, "part1")


print("=" * 60)
print("PART 1 — Yellow Taxi Batch Pipeline")
print("=" * 60)

print("\n[1/5] Reading...")
reader = TaxiReader("part1/input/yellow_tripdata_2025-01.parquet")
df = reader.read()

print("\n[2/5] Validating...")
validator = TaxiValidator()
df = validator.validate(df)

print("\n[3/5] Processing...")
processor = TaxiProcessor()
df = processor.process(df)

print("\n[4/5] Backup validating...")
backup = TaxiBackupValidator()
passed = backup.validate(df)
if not passed:
    print("BACKUP VALIDATION FAILED — stopping pipeline.")
    sys.exit(1)

print("\n[5/5] Writing...")
writer = TaxiWriter(
    local_output_dir="part1/output",
    azure_connection_str=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
)
output = writer.write(df)

print("\n" + "=" * 60)
print(f"DONE — output saved to: {output}")
print(f"Final shape: {df.shape}")
print("=" * 60)
