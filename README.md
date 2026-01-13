# LDJ - Data Jobs ETL Pipeline

A data pipeline that ingests job posting data from CSV, transforms it into a normalized 3NF relational model, and loads it into PostgreSQL.

## Project Structure

```
├── README.md
├── airflow
│   ├── config
│   ├── dags
│   │   ├── __pycache__
│   │   └── data_jobs_pipeline.py
│   ├── logs
│   │   ├── dag_id=data_jobs_pipeline
│   │   ├── dag_processor_manager
│   │   └── scheduler
│   └── plugins
├── credentials.json
├── data
│   └── data_jobs.csv
├── data_ingestion.sql
├── docker
│   ├── docker
│   │   └── init-multiple-dbs.sh
│   ├── docker-compose.yml
│   └── init-multiple-dbs.sh
├── notebooks
│   └── data_pipeline.ipynb
├── requirements.txt
├── schema
│   ├── er_diagram.dbml
│   └── schema.sql
├── src
│   ├── __init__.py
│   ├── __pycache__
│   │   ├── __init__.cpython-313.pyc
│   │   ├── config.cpython-313.pyc
│   │   ├── data_pipeline.cpython-313.pyc
│   │   └── path.cpython-313.pyc
│   ├── config.py
│   ├── data_pipeline.py
│   ├── ingestion.py
│   ├── models.py
│   ├── transformation.py
│   └── utils.py
└── tests
    └── __init__.py
```

## Setup

### Prerequisites

- Python 3.13+
- Docker and Docker Compose
- Apache Airflow

### 1. Environment Setup

```bash
# Navigate to project directory
cd ldj

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Setup

```bash
# Start PostgreSQL container
docker compose up -d

# Verify container is running
docker compose ps
```

### 3. Configure Environment

Copy `.env.example` to `.env` and update credentials if needed:

```bash
cp .env.example .env
```

## Running the Pipeline

Launch the python file for running the ETL

```bash
data_pipeline.py
```

## Running Tests

```bash
pytest tests/ -v
```

## Database Schema

The pipeline transforms flat CSV data into a normalized 3NF model with the following tables:

## 📦 Database Schema (3NF)

The pipeline normalizes the raw CSV into a 3NF relational model.  
The following tables are created and populated by the ETL:

### Dimension Tables
- `companies` — Unique companies that posted jobs
- `locations` — Geographic job location dimension
- `platforms` — Job posting platform dimension
- `schedule_types` — Work schedule type dimension (e.g. full-time, contract)
- `skill_types` — Skill category/type dimension
- `skills` — Individual skills dimension (e.g. python, aws)

### Fact Table
- `jobs` — Main job postings fact table (one row per job)

### Bridge / Junction Tables
- `bridge_job_skill` — Many-to-many relationship between `jobs` and `skills`
- `job_skills` — Staging exploded skill list prior to bridging

### Staging Layer
- `data_jobs_raw` — Cleaned staging data loaded from CSV before dimensional modeling

### ER Diagram
See: `schema/er_diagram.dbml` for the complete diagram and relationships.

See `schema/er_diagram.dbml` for the complete ER diagram.

