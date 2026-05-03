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

> **Note — Worker not starting:** The Celery worker depends on the API server being healthy before it starts. On first run the API server can take 3–4 minutes to initialize, causing the worker to stay in `Created` state and never start. If DAG runs appear queued but never execute, check with:
> ```bash
> docker ps --filter "name=airflow-docker-airflow-worker-1"
> ```
> If the status shows `Created` instead of `Up`, start it manually:
> ```bash
> docker start airflow-docker-airflow-worker-1
> ```

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
3. Copy the generated file into the input folder — the script saves to `part2/sample_data/`, **not** `part2/input/`:
   ```bash
   cp de_project/part2/sample_data/employee_records_unclean.csv de_project/part2/input/
   ```
4. The FileSensor detects it and the pipeline runs automatically
5. Processed file appears in `part2/output/`, input file moves to `part2/input/archived/`

> **Note — Azure output locations:** Part 1 uploads to the `taxi-output` container, Part 2 uploads to `employee-output`.

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

## Troubleshooting

### Airflow slowing down / connection pool exhaustion
If Airflow becomes unresponsive with `QueuePool limit of size 5 overflow 10 reached` errors, it means too many simultaneous DAG runs are exhausting Postgres connections. This typically happens when a scheduled DAG accumulates a backlog of queued runs while the worker was down.

Fix: pause both DAGs, kill all running runs, then unpause and re-trigger cleanly.
```bash
docker exec airflow-docker-airflow-scheduler-1 airflow dags pause employee_realtime_pipeline
docker exec airflow-docker-airflow-scheduler-1 airflow dags pause yellow_taxi_batch_pipeline
```
Then in the Airflow UI go to **Dag Runs**, filter by `running`, select all and mark as failed.

### Init container stuck on restart
After `docker compose restart`, `airflow-init` runs again. If the DB connection pool is already exhausted by other services, init will hang indefinitely. Since migrations are already done, just force-stop it:
```bash
docker stop airflow-docker-airflow-init-1
```
Then restart any unhealthy services:
```bash
docker restart airflow-docker-airflow-apiserver-1 airflow-docker-airflow-worker-1 airflow-docker-airflow-triggerer-1 airflow-docker-airflow-dag-processor-1
```

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
