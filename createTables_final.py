import psycopg2

# Database connection setup
DATABASE_URI = 'YOUR_DATABASE_URI'

# SQL statement to create the stations table
create_stations_table_sql = """
CREATE TABLE IF NOT EXISTS stations (
    station_id VARCHAR(255) PRIMARY KEY,
    station_name VARCHAR(255) NOT NULL,
    latitude NUMERIC(10, 6),  -- Latitude of the station
    longitude NUMERIC(10, 6)  -- Longitude of the station
);
"""

# SQL statement to create the rides table with foreign keys for stations
create_rides_table_sql = """
CREATE TABLE IF NOT EXISTS rides (
    id SERIAL PRIMARY KEY,
    ride_id VARCHAR(255) NOT NULL,
    rideable_type VARCHAR(255) NULL,
    started_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ended_at TIMESTAMP WITHOUT TIME ZONE NULL,
    start_station_id VARCHAR(255) NULL,
    end_station_id VARCHAR(255) NULL,
    start_station_name VARCHAR(255) NULL,
    end_station_name VARCHAR(255) NULL,
    start_lat DECIMAL(9, 6) NULL,
    start_lng DECIMAL(9, 6) NULL,
    end_lat DECIMAL(9, 6) NULL,
    end_lng DECIMAL(9, 6) NULL,
    member_casual VARCHAR(50) NULL,
    trip_duration_seconds INT NULL,
    bike_id VARCHAR(255) NULL,
    gender INT NULL,
    birth_year INT NULL,
    data_source_city VARCHAR(255) NOT NULL
);
"""

def create_tables():
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URI)
        cursor = conn.cursor()
        
        # Execute the create table SQL statements
        cursor.execute(create_stations_table_sql)
        cursor.execute(create_rides_table_sql)
        
        # Commit the changes
        conn.commit()
        print("Tables 'stations' and 'rides' created successfully.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection to clean up
        cursor.close()
        conn.close()

create_tables()
