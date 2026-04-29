import sys
import os
import logging
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / "airflow-docker" / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

sys.path.insert(0, "part2")

from reader import EmployeeReader
from validator import EmployeeValidator
from processor import EmployeeProcessor
from backup_validator_writer import EmployeeBackupValidator, EmployeeWriter

print("=" * 60)
print("PART 2 — Employee Real-Time Pipeline")
print("=" * 60)

# Generate test data if it doesn't exist yet
sample_path = "sample_data/employee_records_unclean.csv"
if not os.path.exists(sample_path):
    print("Generating sample data...")
    os.makedirs("part2/sample_data", exist_ok=True)
    exec(open("part2/generate_sample_data.py").read())

# Step 1 — Read
print("\n[1/5] Reading...")
reader = EmployeeReader(sample_path)
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
writer = EmployeeWriter(
    local_output_dir="part2/output",
    azure_connection_str=os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
)
output = writer.write(df)

print("\n" + "=" * 60)
print(f"DONE — output saved to: {output}")
print(f"Final shape: {df.shape}")
print("=" * 60)
