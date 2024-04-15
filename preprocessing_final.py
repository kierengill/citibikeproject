import os
import pandas as pd
import uuid
import shutil

# Main script execution
base_dir = 'YOUR_BASE_DIR'
nyc_dir = os.path.join(base_dir, 'nyc_data')
jersey_city_dir = os.path.join(base_dir, 'jersey_city_data')

# New directory for preprocessed CSVs ready for COPY
preprocessed_csv_dir = os.path.join(base_dir, 'preprocessed_for_copy')
os.makedirs(preprocessed_csv_dir, exist_ok=True)

# Function to generate UUIDs
def generate_uuid(started_at, start_station_id, bike_id):
    unique_string = f"{started_at}-{start_station_id}-{bike_id}"
    return uuid.uuid5(uuid.NAMESPACE_DNS, unique_string).hex

# Modified function to preprocess CSV files and save them for COPY
def preprocess_and_save_csv_for_copy(directory, city_name):
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            print(f"Processing file: {file_path}")

             # Assume new format unless specified in the filename
            is_old_format = 'old' in filename.lower()
            datetime_cols_indices = [1, 2] if is_old_format else [2, 3]
            converters = {5: str, 7: str} if not is_old_format else {}
            df = pd.read_csv(file_path, parse_dates=datetime_cols_indices, converters=converters)

            # Assign column names based on format and position
            if is_old_format:
                df.columns = ['trip_duration_seconds', 'started_at', 'ended_at',
                              'start_station_id', 'start_station_name', 'start_lat',
                              'start_lng', 'end_station_id', 'end_station_name', 'end_lat',
                              'end_lng', 'bike_id', 'user_type', 'birth_year', 'gender']
                df['member_casual'] = df['user_type'].map({'Subscriber': 'casual', 'Customer': 'member'})
                df.drop('user_type', axis=1, inplace=True)  # Remove the user_type column
                df['rideable_type'] = pd.NA
                df['ride_id'] = df.apply(lambda x: generate_uuid(x['started_at'], x['start_station_id'], x['bike_id']), axis=1)
                df['birth_year'] = df['birth_year'].replace({r'\\N': pd.NA, r'\N': pd.NA})
                df['birth_year'] = df['birth_year'].astype('Int64')
            else:  # New format
                df.columns = ['ride_id', 'rideable_type', 'started_at', 'ended_at',
                              'start_station_name', 'start_station_id', 'end_station_name',
                              'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng',
                              'member_casual']
                df['trip_duration_seconds'] = pd.NA
                df['bike_id'] = pd.NA
                df['gender'] = pd.NA
                df['birth_year'] = pd.NA

            df['started_at'] = pd.to_datetime(df['started_at'])
            df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
            df['data_source_city'] = city_name
            # Convert start_station_id and end_station_id to strings before replacing decimal parts
            df['start_station_id'] = df['start_station_id'].astype(str).str.replace(r'\.\d+', '', regex=True)
            df['end_station_id'] = df['end_station_id'].astype(str).str.replace(r'\.\d+', '', regex=True)

            # Define final column order and save preprocessed CSV
            final_columns = ['ride_id', 'rideable_type', 'started_at', 'ended_at',
                             'start_station_name', 'start_station_id', 'end_station_name', 'end_station_id',
                             'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual',
                             'trip_duration_seconds', 'bike_id', 'gender', 'birth_year', 'data_source_city']
            df = df[final_columns]
            preprocessed_csv_path = os.path.join(preprocessed_csv_dir, f"preprocessed_{filename}")
            df.to_csv(preprocessed_csv_path, index=False, na_rep='NULL', header=True)

preprocess_and_save_csv_for_copy(nyc_dir, 'NYC')
preprocess_and_save_csv_for_copy(jersey_city_dir, 'Jersey City')

shutil.rmtree(nyc_dir, ignore_errors=True)
shutil.rmtree(jersey_city_dir, ignore_errors=True)

print("Data preprocessing completed.")