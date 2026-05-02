# Data Engineering Project — 2025-2026

## Structure

```
de_project/
├── part1/                          # Batch Processing — Yellow Taxi
│   ├── reader.py
│   ├── validator.py
│   ├── processor.py
│   ├── backup_validator.py
│   ├── writer.py
│   ├── dags/
│   │   └── taxi_pipeline_dag.py
│   ├── input/                      # Place yellow_tripdata_2025-01.parquet here
│   └── output/                     # Processed files appear here
│
├── part2/                          # Real-Time Processing — Employee Records
│   ├── reader.py
│   ├── validator.py
│   ├── processor.py
│   ├── backup_validator_writer.py
│   ├── generate_sample_data.py     # Run once to create test data
│   ├── dags/
│   │   └── employee_pipeline_dag.py
│   ├── input/                      # Drop CSV/XLSX files here to trigger pipeline
│   │   └── archived/               # Processed files are moved here automatically
│   └── output/                     # Processed files appear here
│
└── README.md
```

---

## Setup

### 1. Install dependencies

```bash
python -m pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp airflow-docker/.env.example airflow-docker/.env
```

Edit `airflow-docker/.env` and fill in:
- `AZURE_STORAGE_CONNECTION_STRING` — your Azure Storage connection string
- `FERNET_KEY` — generate with: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`

### 3. Start Airflow with Docker

```bash
cd airflow-docker
docker compose up -d
```

Open http://localhost:8080 — login: `airflow` / `airflow`

The `part1/` and `part2/` folders are automatically mounted into Airflow via Docker volume mounts — no manual copying needed.

### 4. Place input data (Part 1)

Download `yellow_tripdata_2025-01.parquet` from the NYC TLC website and place it in `part1/input/`.

---

## Running the pipelines

### Part 1 (Batch)
1. Place `yellow_tripdata_2025-01.parquet` in `part1/input/`
2. In Airflow UI, find DAG `yellow_taxi_batch_pipeline`
3. Enable the toggle, or trigger it manually with the ▶ button

### Part 2 (Real-time)
1. Enable DAG `employee_realtime_pipeline` in Airflow UI
2. Generate test data: `python part2/generate_sample_data.py`
3. Drop any CSV or XLSX file into `part2/input/`
4. The FileSensor detects it and the pipeline runs automatically
5. Processed file appears in `part2/output/`, input file moves to `part2/input/archived/`

---

## Validation Rules Summary

### Part 1 — Yellow Taxi

| Column | Rule |
|---|---|
| tpep_pickup_datetime | Non-null, valid datetime |
| tpep_dropoff_datetime | Non-null, valid datetime, > pickup |
| passenger_count | 1–9 |
| trip_distance | 0–500 miles |
| PULocationID / DOLocationID | 1–265 (TLC zones) |
| payment_type | 1–6 |
| fare_amount | 0–1000 |
| total_amount | 0–1000 |

### Part 2 — Employee Records

| Column | Rule |
|---|---|
| employee_id | Non-null, positive, unique |
| full_name | Non-null |
| department | One of: Engineering, Marketing, Sales, HR, Finance, Operations |
| salary_eur | Non-null, >= 0 |
| hire_date | Non-null, valid date |
| email | Valid format (non-mandatory, logged only) |
| performance_score | 1–5 (non-mandatory, logged only) |

---

## Derived Columns

### Part 1
- `trip_duration_minutes` — dropoff minus pickup in minutes
- `average_speed_mph` — distance / (duration_hours), null where duration = 0
- `pickup_year`, `pickup_month`
- `revenue_per_mile` — total_amount / distance, null where distance = 0
- `trip_distance_category` — Short / Medium / Long
- `fare_category` — Low / Medium / High
- `trip_time_of_day` — Night / Morning / Afternoon / Evening

### Part 2
- `gender_normalized` — Male / Female / Unknown
- `salary_band` — Entry / Mid / Senior / Executive
- `seniority_level` — Junior / Mid-level / Senior / Principal / Unknown
- `years_at_company` — computed from hire_date to today
- `is_senior` — True if salary_band is Senior or Executive
