# Citibike Data Project
## Author: Kieren Gill

### Background
In this project, I built a pipeline that downloads Citibike's publicly available data, preprocesses it, and loads it into a PostgreSQL database. As an avid Citibiker myself, I was excited to undertake this project, and it makes me happy to know how publicly accessible Citibike makes this data so that anyone can derive their own insights from it!

If you want to load the data onto your own machine, you should run the files in the following order:

1) ingestion_final.py
2) preprocessing_final.py
3) createTables_final.py
4) loading_final.py
5) createIndexes_final.py (optional)

### Data Ingestion - ingestion_final.py
This Python script automates the data ingestion process for Citibike's publicly available data, facilitating the download, organization, and preparation of data for preprocessing. 

It starts by setting up necessary directories to store downloaded ZIP files and extracted CSV data, categorizing them into New York City and Jersey City datasets. The script uses Selenium for web scraping to identify and download all available ZIP files from Citibike's data repository. After downloading, each ZIP file is extracted to its respective directory based on its filename prefix - files that have the prefix "JC" have data from Jersey city, and files without that prefix contain data from NYC. 

The script then performs several housekeeping tasks: it removes any duplicates and unnecessary nested directories, renames files to indicate whether they conform to the pre- or post-February 2021 data format, and deletes files based on specific naming conventions to maintain a clean dataset.

For the datasets in 2013 and 2018, there were files which had overlapping data. Files in each month that ended with "_1_old.csv", "_2_old.csv" or "_3_old.csv" all contained data in a file that ended with "_old.csv", so the redundant files were removed. Additionally, some files ended with "_old_old.csv", which were also removed because they contained duplicates. Take note that all the data in the deleted files are captured in existing files, so there is no data loss here. 

This ingestion process ensures that the data is ready for preprocessing and further analysis, streamlining the workflow for data analysts who wish to explore Citibike's dataset on bike-sharing usage.

### Data Preprocessing - preprocessing_final.py
The preprocessing begins with setting up a new directory to store the cleaned CSV files, ensuring that any pre-existing data is organized and ready for database loading. Each CSV file within the NYC and Jersey City directories is iteratively processed to accommodate differences in data formats.

For files classified under the old format (prior to February 2021), additional steps are taken to align with the new data structure. This includes mapping the user_type values to a new member_casual column, with "Subscriber" mapped to "casual" and "Customer" to "member", thereby standardizing user type categorization across all datasets. This step involves the removal of the original user_type column and introduction of new columns (ride_id, rideable_type) with appropriate placeholders where data is unavailable. For files classified under the new format, new columns are also introduced (trip_duration_seconds, bike_id, gender, birth_year), so that they align with the database schema during data loading.

In both old and new formats, certain transformations are universal. The start_station_id and end_station_id are converted into string formats, removing any decimal points and trailing numbers to ensure consistency in station identification. Similarly, any non-applicable (NA) or undefined values in the birth_year column are identified and converted into a uniform NA representation, with the column datatype set to integer to reflect the true nature of the data.

The script also addresses data enrichment by generating unique identifiers for each ride in the old format datasets using a combination of start time, start station, and bike ID. This ensures each record can be distinctly identified, mirroring the ride_id present in the new format.

After cleaning and transforming the data, the script arranges the columns in a specified order to match the database schema, ensuring seamless integration during the loading stage. The processed data is then saved into the pre-established directory for preprocessed CSVs, ready for loading the SQL database. The directories initially created for the raw NYC and Jersey City data are cleared upon completion of preprocessing, ensuring the workspace remains organized and focused solely on the data ready for analysis.

This preprocessing script transoforms the uncleaned datasets into a coherent structure, aligning with analytical needs and database requirements. It automates the cleaning, transformation, and preparation of Citibike data, making it an extremely important step in the data pipeline.

### Data Loading - loading_final.py
In this script, the preprocessed data is ingested into a PostgreSQL database, ensuring that the dataset is stored efficiently and is ready for analysis.

The script starts by establishing a connection to the PostgreSQL database using credentials specified in the DATABASE_URI. It then iterates over all preprocessed CSV files stored in a designated directory, loading each file into the rides table within the database. The COPY command is used for this operation, which is efficient in bulk data loading, which is why I chose it for handling this dataset.

The script divides the data into two distinct tables: rides and stations. This division is informed by the principle of data normalization, aimed at reducing redundancy and improving data integrity. It also allows for a more organized and efficient representation of the data, facilitating easier maintenance and querying.

First, data is loaded into the rides table, and the script addresses the potential issue of duplicate records within the rides table. Through a SQL query utilizing a window function (ROW_NUMBER()), duplicates are identified and removed, keeping only the first occurrence of each unique ride_id. This cleanup is crucial for maintaining the database's integrity and ensuring accurate analysis.

After removing duplicates, the script proceeds to set the ride_id column as the primary key of the rides table. This action not only enforces uniqueness but also improves query performance. To accommodate this change, the existing primary key constraint is first dropped, and the id column, initially intended as the primary key, is removed from the table. ride_id was chosen as the primary key instead of an automatically generated SQL primary key so that the primary key would be consistent across imports.

Next, the script loads the stations table with unique station data extracted from the rides table (station_id, station_name, latitude, longitude). The script then introduces foreign key constraints to establish a relational link between the rides and stations tables. By setting nan station IDs to NULL and defining foreign key constraints, it ensures referential integrity and enables cascading updates or deletions. 

This data loading script loads the data ito my PostgreSQL database, and the data is now ready for analysis!

### Additional Scripts

I have three additional scripts:

- createTables_final.py (which creates the rides and the stations table)
- createIndexes_final.py (creates 5 indexes to help speed up querying data)
- dropTables_final.py (drops both the tables if necessary)

### Connecting to my Database

Assuming that you have remote access to my localhost through a network, here is how you can connect to my PostgreSQL database:

```
psql -h <MyMachine'sIP> -p <Port> -U <Username> -d citibike_test
```

In this case, this is what it would look like:

```
psql -h <172.22.32.1> -p <5432> -U <postgres> -d citibike_test
```
Then, you should be able to run sql queries through the terminal interface! You can also connect to my database using PgAdmin or other GUI tools, but you will require the same connection details provided above.

### SQL Table Structure

Here's the schema for both the rides and the stations tables respectively:

#### Table: Rides

|   Column             |            Type             | Collation | Nullable | Default |
| -------------------- | --------------------------- | --------- | -------- | ------- |
 ride_id               | character varying(255)      |           | not null |         |
 rideable_type         | character varying(255)      |           |          |         |
 started_at            | timestamp without time zone |           | not null |         |
 ended_at              | timestamp without time zone |           |          |         |
 start_station_id      | character varying(255)      |           |          |         |
 end_station_id        | character varying(255)      |           |          |         |
 start_station_name    | character varying(255)      |           |          |         |
 end_station_name      | character varying(255)      |           |          |         |
 start_lat             | numeric(9,6)                |           |          |         |
 start_lng             | numeric(9,6)                |           |          |         |
 end_lat               | numeric(9,6)                |           |          |         |
 end_lng               | numeric(9,6)                |           |          |         |
 member_casual         | character varying(50)       |           |          |         |
 trip_duration_seconds | integer                     |           |          |         |
 bike_id               | character varying(255)      |           |          |         |
 gender                | integer                     |           |          |         |
 birth_year            | integer                     |           |          |         |
 data_source_city      | character varying(255)      |           | not null |         |

<br>

Indexes: 
- "rides_pkey" PRIMARY KEY, btree (ride_id)

Foreign-key constraints:

- "fk_end_station" FOREIGN KEY (end_station_id) REFERENCES stations(station_id) ON DELETE SET NULL

- "fk_start_station" FOREIGN KEY (start_station_id) REFERENCES stations(station_id) ON DELETE SET NULL

<br>

#### Table: Stations

|    Column    |          Type          | Collation | Nullable | Default |
| ------------ | ---------------------- | --------- | -------- | ------- |
| station_id   | character varying(255) |           | not null |         |
| station_name | character varying(255) |           | not null |         |
| latitude     | numeric(10,6)          |           |          |         |
| longitude    | numeric(10,6)          |           |          |         |

<br>

Indexes: 
- "stations_pkey" PRIMARY KEY, btree (station_id)

Referenced by:

- TABLE "rides" CONSTRAINT "fk_end_station" FOREIGN KEY (end_station_id) REFERENCES stations(station_id) ON DELETE SET NULL

- TABLE "rides" CONSTRAINT "fk_start_station" FOREIGN KEY (start_station_id) REFERENCES stations(station_id) ON DELETE SET NULL



### Final Remarks
Thank you for taking the time to go over my project! If I could change anything, I would try and host this on a domain. I would have also tried to use Apache Spark - I wonder how much faster it would be for managing data of this volume.