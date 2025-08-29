import os
import psycopg2
from psycopg2.extras import execute_values, DictCursor
from dotenv import load_dotenv
from typing import List, Dict, Any
import datetime
from api_request import mock_data

# The final code will assume the .env file is loaded and visible to Airflow
# by being located in the appropriate container path.

def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database using environment variables.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST', 'postgres'),  # Default to 'postgres' for Docker
            port=os.getenv('DB_PORT', '5432'),
            connect_timeout=5  # Add a connection timeout
        )
        print("Successfully connected to the PostgreSQL database.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        # Print the connection string for debugging (without password)
        print(f"Connection string: postgresql://{os.getenv('DB_USER')}:****@{os.getenv('DB_HOST', 'postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}")

def create_table_if_not_exists(cursor: DictCursor):
    """
    Creates the weather data schema and table if they do not already exist.
    """
    try:
        schema_name = os.getenv('DB_SCHEMA', 'weather_api')
        table_name = os.getenv('DB_TABLE', 'weather_api_raw_data')
        
        # Use an f-string for schema creation as it's a simple, non-dynamic value
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
        cursor.execute(create_schema_query)
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            location_name VARCHAR(100),
            location_country VARCHAR(100),
            location_region VARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            timezone_id VARCHAR(100),
            observation_timestamp TIMESTAMP,
            current_temperature_celsius FLOAT,
            weather_description TEXT,
            humidity_percentage INT,
            wind_speed_kmh FLOAT,
            pressure_mb FLOAT,
            precipitation_mm FLOAT,
            visibility_km FLOAT,
            uv_index INT,
            air_quality_co FLOAT,
            air_quality_no2 FLOAT,
            air_quality_o3 FLOAT,
            air_quality_so2 FLOAT,
            air_quality_pm2_5 FLOAT,
            air_quality_pm10 FLOAT,
            air_quality_us_epa_index INT,
            air_quality_gb_defra_index INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        
        # Committing the transaction to finalize the schema and table creation.
        cursor.connection.commit()
    except psycopg2.Error as e:
        print(f"An error occurred while creating the table: {e}")
        # Rollback the transaction on error to prevent partial changes.
        cursor.connection.rollback()

def insert_data_to_db(cursor: DictCursor, data: List[Dict[str, Any]]):
    """
    Inserts weather data into the database in bulk using `execute_values`.
    
    Args:
        cursor (DictCursor): A psycopg2 cursor object for database operations.
        data (List[Dict[str, Any]]): A list of dictionaries containing weather data.
    """
    try:
        schema_name = os.getenv('DB_SCHEMA', 'weather_api')
        table_name = os.getenv('DB_TABLE', 'weather_api_raw_data')
        
        # Define the column names to insert into.
        columns = [
            "location_name", "location_country", "location_region", "latitude", "longitude",
            "timezone_id", "observation_timestamp", "current_temperature_celsius",
            "weather_description", "humidity_percentage", "wind_speed_kmh",
            "pressure_mb", "precipitation_mm", "visibility_km", "uv_index",
            "air_quality_co", "air_quality_no2", "air_quality_o3", "air_quality_so2",
            "air_quality_pm2_5", "air_quality_pm10", "air_quality_us_epa_index",
            "air_quality_gb_defra_index"
        ]
        
        values_to_insert = []
        for d in data:
            location = d.get('location', {})
            current = d.get('current', {})
            air_quality = current.get('air_quality', {})
            # Convert localtime string to a datetime object
            localtime_str = location.get('localtime')
            observation_timestamp = None
            if localtime_str:
                # The format is "YYYY-MM-DD HH:MM"
                observation_timestamp = datetime.datetime.strptime(localtime_str, '%Y-%m-%d %H:%M')
            
            # Handle weather_descriptions which is a list
            weather_description = ', '.join(current.get('weather_descriptions')) if current.get('weather_descriptions') else None
            
            # Prepare a list of values in the same order as `columns`
            values = [
                location.get('name'),
                location.get('country'),
                location.get('region'),
                float(location.get('lat')) if location.get('lat') else None,
                float(location.get('lon')) if location.get('lon') else None,
                location.get('timezone_id'),
                observation_timestamp, # Now a datetime object
                float(current.get('temperature')) if current.get('temperature') is not None else None,
                weather_description,
                current.get('humidity'),
                float(current.get('wind_speed')) if current.get('wind_speed') is not None else None,
                float(current.get('pressure')) if current.get('pressure') is not None else None,
                float(current.get('precip')) if current.get('precip') is not None else None,
                float(current.get('visibility')) if current.get('visibility') is not None else None,
                current.get('uv_index'),
                float(air_quality.get('co')) if air_quality.get('co') is not None else None,
                float(air_quality.get('no2')) if air_quality.get('no2') is not None else None,
                float(air_quality.get('o3')) if air_quality.get('o3') is not None else None,
                float(air_quality.get('so2')) if air_quality.get('so2') is not None else None,
                float(air_quality.get('pm2_5')) if air_quality.get('pm2_5') is not None else None,
                float(air_quality.get('pm10')) if air_quality.get('pm10') is not None else None,
                int(air_quality.get('us-epa-index')) if air_quality.get('us-epa-index') is not None else None,
                int(air_quality.get('gb-defra-index')) if air_quality.get('gb-defra-index') is not None else None
            ]
            values_to_insert.append(values)
        
        # Build the insert query with placeholders.
        insert_query = f"""
        INSERT INTO {schema_name}.{table_name} ({', '.join(columns)})
        VALUES %s;
        """
        
        # Execute the bulk insert using `execute_values`.
        execute_values(cursor, insert_query, values_to_insert)
        # Commit the transaction after the successful insert.
        cursor.connection.commit()

    except psycopg2.Error as e:
        print(f"An error occurred while inserting data: {e}")
        cursor.connection.rollback()
    
def final_main():
    """
    Main function to orchestrate the database operations. This is the function
    that will be called by the Airflow PythonOperator.
    """
    # Load environment variables at the start of the function call
    load_dotenv()
    # The raw JSON data provided by the user
    raw_api_response = mock_data()[0]  # Get the first (and only) dictionary from the list    
    try:
        # Use a `with` statement to ensure the connection is closed automatically.
        with get_db_connection() as conn:
            # Use `DictCursor` to return results as dictionaries.
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                create_table_if_not_exists(cursor)
                
                # We need to wrap the single dictionary in a list because execute_values
                # expects an iterable of records.
                formatted_data_to_insert = [raw_api_response]

                # Now insert the correctly formatted data
                insert_data_to_db(cursor, formatted_data_to_insert)

    except Exception as e:
        print(f"An error occurred during database operations: {e}")