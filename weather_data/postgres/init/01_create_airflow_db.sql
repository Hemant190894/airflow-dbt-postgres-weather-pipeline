-- Create Airflow database if it doesn't exist (idempotent)
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT 1 FROM pg_database WHERE datname = 'weather_airflow'
   ) THEN
      EXECUTE 'CREATE DATABASE weather_airflow';
   END IF;
END
$$;
