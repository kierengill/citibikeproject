import psycopg2

# Database connection setup
DATABASE_URI = 'YOUR_DATABASE_URI'

# SQL statements to create indexes
index_creation_commands = [
    "CREATE INDEX IF NOT EXISTS idx_rides_started_at ON rides USING BTREE (started_at);",
    "CREATE INDEX IF NOT EXISTS idx_rides_ended_at ON rides USING BTREE (ended_at);",
    "CREATE INDEX IF NOT EXISTS idx_rides_start_station_id ON rides USING BTREE (start_station_id);",
    "CREATE INDEX IF NOT EXISTS idx_rides_end_station_id ON rides USING BTREE (end_station_id);",
    "CREATE INDEX IF NOT EXISTS idx_rides_member_casual ON rides USING BTREE (member_casual);",
]

def create_indexes():
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URI)
        cursor = conn.cursor()
        
        for command in index_creation_commands:
            cursor.execute(command)

        # Commit the changes
        conn.commit()
        print("Indexes created successfully.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection to clean up
        cursor.close()
        conn.close()

create_indexes()
