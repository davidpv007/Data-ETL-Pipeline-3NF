# LDJ - Data Jobs ETL Pipeline

A data pipeline that ingests job posting data from CSV, transforms it into a normalized 3NF relational model, and loads it into PostgreSQL.

## Project Structure

```
ldj/
├── docker-compose.yml          # PostgreSQL container
├── .env                        # Environment variables (DB credentials)
├── .env.example                # Template for .env
├── requirements.txt            # Python dependencies
├── README.md                   # Documentation
├── data/
│   └── data_jobs.csv           # Source data file
├── notebooks/
│   └── data_pipeline.ipynb     # Main Jupyter notebook for pipeline
├── src/
│   ├── __init__.py
│   ├── config.py               # Configuration and env loading
│   ├── ingestion.py            # Phase 1: CSV ingestion logic
│   ├── transformation.py       # Phase 2: 3NF transformation logic
│   ├── models.py               # SQLAlchemy models
│   └── utils.py                # Logging and helper functions
├── tests/
│   ├── __init__.py
│   ├── test_ingestion.py
│   └── test_transformation.py
└── schema/
    ├── er_diagram.dbml         # ER diagram source (for dbdiagram.io)
    └── schema.sql              # DDL for 3NF model
```

## Setup

### Prerequisites

- Python 3.13+
- Docker and Docker Compose

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

Launch Jupyter Lab and open the notebook:

```bash
jupyter lab notebooks/data_pipeline.ipynb
```

## Running Tests

```bash
pytest tests/ -v
```

## Database Schema

The pipeline transforms flat CSV data into a normalized 3NF model with the following tables:

- **companies** - Unique company dimension
- **locations** - Job location dimension
- **platforms** - Job posting platform dimension
- **schedule_types** - Work schedule type dimension
- **skill_types** - Skill category dimension
- **skills** - Individual skills with type reference
- **jobs** - Main job postings fact table
- **job_skills** - Many-to-many junction table

See `schema/er_diagram.dbml` for the complete ER diagram.

