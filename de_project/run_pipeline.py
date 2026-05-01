"""
Script to run Part 2: Employee Real-Time Ingestion Pipeline.
"""

# pylint: disable=duplicate-code
import logging
import os
import sys

from backup_validator_writer import (
    EmployeeBackupValidator,
    EmployeeWriter,
)  # pylint: disable=import-error
from processor import EmployeeProcessor  # pylint: disable=import-error
from reader import EmployeeReader  # pylint: disable=import-error
from validator import EmployeeValidator  # pylint: disable=import-error

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

sys.path.insert(0, "part2")


print("=" * 60)
print("PART 2 — Employee Real-Time Pipeline")
print("=" * 60)

# First generate the test data if it doesn't exist

if not os.path.exists("part2/sample_data/employee_records_unclean.csv"):
    print("Generating sample data...")
    with open("part2/generate_sample_data.py", encoding="utf-8") as f:
        exec(f.read())  # pylint: disable=exec-used

# Step 1 — Read
print("\n[1/5] Reading...")
reader = EmployeeReader("part2/sample_data/employee_records_unclean.csv")
df = reader.read()

# Step 2 — Validate
print("\n[2/5] Validating...")
validator = EmployeeValidator()
df = validator.validate(df)

# Step 3 — Process
print("\n[3/5] Processing...")
processor = EmployeeProcessor()
df = processor.process(df)

# Step 4 — Backup Validate
print("\n[4/5] Backup validating...")
backup = EmployeeBackupValidator()
passed = backup.validate(df)
if not passed:
    print("BACKUP VALIDATION FAILED — stopping pipeline.")
    sys.exit(1)

# Step 5 — Write
print("\n[5/5] Writing...")
writer = EmployeeWriter(local_output_dir="part2/output", azure_connection_str=None)
output = writer.write(df)

print("\n" + "=" * 60)
print(f"DONE — output saved to: {output}")
print(f"Final shape: {df.shape}")
print("=" * 60)
