import os
import psycopg2

# Database connection setup
DATABASE_URI = "host='HOSTNAME' dbname='DATABASENAME' user='USERNAME' password='YOUR_PASSWORD'"
conn = psycopg2.connect(DATABASE_URI)
cursor = conn.cursor()

base_dir = 'YOUR_BASE_DIR'
preprocessed_csv_dir = os.path.join(base_dir, 'preprocessed_for_copy')

# Function to insert unique station data into the 'stations' table.
# It selects distinct station information from the 'rides' table to avoid duplicates.
def deduplicate_and_load_stations():
    print("Deduplicating and loading stations...")
    # Extract unique station information from rides table and load into stations table
    cursor.execute("""
        INSERT INTO stations (station_id, station_name, latitude, longitude)
        SELECT DISTINCT start_station_id, start_station_name, start_lat, start_lng FROM rides
        WHERE start_station_id IS NOT NULL AND start_station_name IS NOT NULL
        UNION
        SELECT DISTINCT end_station_id, end_station_name, end_lat, end_lng FROM rides
        WHERE end_station_id IS NOT NULL AND end_station_name IS NOT NULL
        ON CONFLICT (station_id) DO NOTHING;
    """)
    conn.commit()

# Removes duplicate rides based on the 'ride_id' column.
# Additionally, it sets 'ride_id' as the primary key and drops the old 'id' column.
def remove_ride_duplicates_and_set_primary_key():
    print("Removing duplicates based on ride_id...")
    # Remove duplicate ride_ids while keeping the earliest ride based on started_at
    cursor.execute("""
    WITH ranked_rides AS (
        SELECT
            id,  -- Assuming 'id' is the unique identifier for each row in the 'rides' table
            ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY started_at) AS rn
        FROM
            rides
    )
    DELETE FROM
        rides
    WHERE
        id IN (SELECT id FROM ranked_rides WHERE rn > 1);
    """)
    conn.commit()
    
    print("Dropping existing primary key constraint, if any...")
    cursor.execute("ALTER TABLE rides DROP CONSTRAINT IF EXISTS rides_pkey;")
    conn.commit()

    print("Setting ride_id as primary key...")
    # Now that duplicates are removed and any existing primary key constraint is dropped, set ride_id as the new primary key
    cursor.execute("""
    ALTER TABLE rides
    ADD CONSTRAINT rides_pkey PRIMARY KEY (ride_id);
    """)
    conn.commit()

    print("Dropping the id column...")
    cursor.execute("ALTER TABLE rides DROP COLUMN id;")
    conn.commit()

    print("Operation completed successfully.")

# Adding foreign key constraints to the 'rides' table
def add_foreign_key_constraints():
    print("Setting 'nan' station IDs to NULL...")
    # Set 'nan' station IDs to NULL for start_station_id
    cursor.execute("""
        UPDATE rides
        SET start_station_id = NULL
        WHERE start_station_id = 'nan';
    """)
    
    # Set 'nan' station IDs to NULL for end_station_id
    cursor.execute("""
        UPDATE rides
        SET end_station_id = NULL
        WHERE end_station_id = 'nan';
    """)
    conn.commit()
    print("'nan' station IDs set to NULL successfully.")

    print("Adding foreign key constraints...")
    # Add foreign key constraint for start_station_id
    cursor.execute("""
        ALTER TABLE rides
        ADD CONSTRAINT fk_start_station
        FOREIGN KEY (start_station_id) 
        REFERENCES stations(station_id)
        ON DELETE SET NULL;
    """)
    
    # Add foreign key constraint for end_station_id
    cursor.execute("""
        ALTER TABLE rides
        ADD CONSTRAINT fk_end_station
        FOREIGN KEY (end_station_id) 
        REFERENCES stations(station_id)
        ON DELETE SET NULL;
    """)
    conn.commit()
    print("Foreign key constraints added successfully.")


# Loads ride data from CSV files into the 'rides' table
def load_rides(filepath):
    print("Loading rides...")
    with open(filepath, 'r') as f:
        # Skip the header row and use COPY to load data directly
        next(f)
        cursor.copy_expert(sql=f"""
        COPY rides(ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, 
                   end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual, 
                   trip_duration_seconds, bike_id, gender, birth_year, data_source_city)
        FROM STDIN WITH CSV HEADER NULL 'NULL'
        """, file=f)
    conn.commit()

# Loads all preprocessed CSV files into the database
for filename in os.listdir(preprocessed_csv_dir):
    if filename.endswith('.csv'):
        filepath = os.path.join(preprocessed_csv_dir, filename)
        print(f"Processing file: {filepath}")
        load_rides(filepath)  # Load ride data

# After loading all ride data, deduplicate and load stations, and add foreign key constraints
remove_ride_duplicates_and_set_primary_key()
deduplicate_and_load_stations()
add_foreign_key_constraints()

cursor.close()
conn.close()

print("Data loading completed.")
