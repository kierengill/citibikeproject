import psycopg2

# Database connection setup
DATABASE_URI = 'YOUR_DATABASE_URI'

# SQL statement to drop the stations table
drop_stations_table_sql = """
DROP TABLE IF EXISTS rides;
"""

# SQL statement to drop the rides table with foreign keys for stations
drop_rides_table_sql = """
DROP TABLE IF EXISTS stations;
"""

def drop_tables():
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URI)
        cursor = conn.cursor()
        
        # Execute the drop table SQL statements
        cursor.execute(drop_stations_table_sql)
        cursor.execute(drop_rides_table_sql)
        
        # Commit the changes
        conn.commit()
        print("Tables 'stations' and 'rides' dropped successfully.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection to clean up
        cursor.close()
        conn.close()

drop_tables()
