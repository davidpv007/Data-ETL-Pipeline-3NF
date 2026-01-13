#!/bin/sh
set -e

# Create data_jobs_db database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE data_jobs_db;
EOSQL

# Create airflow_db database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE airflow_db;
EOSQL

echo "Databases data_jobs_db and airflow_db created successfully!"

