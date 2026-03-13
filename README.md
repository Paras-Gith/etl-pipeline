# etl-pipeline
Automated CSV to Database ETL Pipeline using Python, Airflow and Docker

# Automated CSV → Database ETL Pipeline

# Overview
Automated ETL pipeline to process CSV/Excel files into MySQL/PostgreSQL databases using Python, Apache Airflow, and Docker.

# Architecture
CSV/Excel Files → Ingestion → Validation → Cleaning → Loading → Reports

# Tech Stack
- Python (pandas, SQLAlchemy)
- Apache Airflow (orchestration)
- PostgreSQL / MySQL
- Docker & Docker Compose

# Project Structure
etl_pipeline/
├── dags/          # Airflow DAG definitions
├── src/           # ETL pipeline modules
├── config/        # Schema & DB configuration
├── data/          # Input/output data
├── logs/          # Pipeline logs
└── reports/       # Generated reports

# How to Run
1. Clone the repo
2. Run: docker-compose up airflow-init
3. Run: docker-compose up
4. Open: http://localhost:8080 (airflow/airflow)
5. Trigger the csv_to_db_etl DAG

# Features
- Auto-detects CSV and Excel files
- Schema validation and error recovery
- Retry logic with exponential backoff
- Automated daily scheduling
- HTML report generation
