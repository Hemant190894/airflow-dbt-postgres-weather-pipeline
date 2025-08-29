#!/bin/bash
set -euo pipefail

# This script runs only on first-time cluster init (empty PGDATA)
# Create AIRFLOW_DB if it doesn't exist yet
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
DO
$$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '${AIRFLOW_DB}') THEN
      EXECUTE format('CREATE DATABASE %I', '${AIRFLOW_DB}');
   END IF;
END
$$;
SQL
