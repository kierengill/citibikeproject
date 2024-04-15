from os import makedirs, remove, rmdir
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, unquote
from re import findall
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bs
from zipfile import ZipFile
from pathlib import Path
import requests
import shutil
import os

# Function to extract ZIP files into a designated directory based on the filename prefix
def extract_zip(filename: str, zip_path: str) -> None:
    # Assign directory based on whether the filename indicates Jersey City or NYC data
    # Files that start with JC contain Jersey City data, otherwise it contains NYC data
    if filename.startswith("JC"):
        extract_dir = "./jersey_city_data"
    else:
        extract_dir = "./nyc_data"

    # Extract the ZIP file and then delete it to save space
    with ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_dir)
    remove(zip_path)

# Function to download ZIP files from a given URL and then call extract_zip
def download_and_extract(href: str, fname: str) -> None:
    response = requests.get(href, stream=True)
    zip_path = f"./zips/{fname}.zip"
    
    # Save the ZIP file and then extract its contents
    with open(zip_path, "wb") as fd:
        for chunk in response.iter_content(chunk_size=128):
            fd.write(chunk)
    extract_zip(fname, zip_path)

# Function to remove duplicates and move files from nested directories to a base directory
def remove_duplicates_and_move(base_directory: str):
    # Remove a common macOS directory that might be included in ZIP files
    macosx_dir = Path(base_directory) / '__MACOSX'
    if macosx_dir.exists():
        shutil.rmtree(macosx_dir)
        print(f"Removed __MACOSX directory: {macosx_dir}")

    seen_files = {}
    base_path = Path(base_directory)

    # Remove duplicate CSV files and ensure all files are directly under the base directory
    for file_path in base_path.rglob('*.csv'):
        if file_path.name not in seen_files:
            seen_files[file_path.name] = file_path
        else:
            file_path.unlink()

    for file_path in seen_files.values():
        if file_path.parent != base_path:
            shutil.move(str(file_path), str(base_path))

    # Remove any empty directories left behind
    for dir_path in base_path.iterdir():
        if dir_path.is_dir():
            shutil.rmtree(dir_path)

# Function to rename files based on when citibike changed their data format
# Citibike changed their data format in Feb 2021
def rename_files(base_directory: str):
    base_path = Path(base_directory)
    for file_path in base_path.glob('*.csv'):
        filename = file_path.stem
        extension = file_path.suffix

        # Rename old NYC data files to include '_old' suffix for easy identification
        if base_directory.endswith("nyc_data"):
            try:
                file_date = int(filename[:6])
                if file_date < 202102:
                    new_filename = f"{filename}_old{extension}"
                    file_path.rename(base_path / new_filename)
            except ValueError:
                pass

        # Similar renaming logic for Jersey City data
        elif base_directory.endswith("jersey_city_data"):
            try:
                start_index = filename.find('JC-') + 3
                file_date_str = filename[start_index:start_index+6]
                file_date = int(file_date_str)
                
                date_valid = True
            except ValueError:
                date_valid = False
            
            if not date_valid:
                try:
                    parts = filename.split("-")
                    file_date = int(parts[1])
                except (ValueError, IndexError):
                    file_date = None

            if file_date and file_date < 202102:
                new_filename = f"{filename}_old{extension}"
                file_path.rename(base_path / new_filename)

# Function to perform additional file clean-up based on naming conventions
def clean_up_files(base_directory: str):
    base_path = Path(base_directory)

    # Gather all csv files in the directory
    csv_files = list(base_path.glob('*.csv'))

    # Filter and delete specific files
    for file_path in csv_files:
        # Extract year and check if it's 2013 or 2018
        year = file_path.stem[:4]
        if year in ['2013', '2018']:
            # If file ends with '1_old', '2_old', or '3_old', delete it
            if any(file_path.stem.endswith(f"{i}_old") for i in range(1, 4)):
                print(f"Deleting {file_path.name} based on year and suffix condition.")
                file_path.unlink()

        # Additionally, delete any file ending with '_old_old.csv'
        if '_old_old' in file_path.stem:
            print(f"Deleting {file_path.name} for ending with '_old_old'.")
            file_path.unlink()

if __name__ == '__main__':
    # Initial setup: creating necessary directories
    makedirs("zips", exist_ok=True)
    makedirs("nyc_data", exist_ok=True)
    makedirs("jersey_city_data", exist_ok=True)

    nyc_data_directory = 'nyc_data'
    jersey_city_directory = 'jersey_city_data'

    # Web scraping setup to download Citibike data ZIP files
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    
    driver = webdriver.Chrome(options=options)
    driver.get("https://s3.amazonaws.com/tripdata/index.html")

    # Attempt to download and process all listed ZIP files
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'a')))
        
        filenames = []
        for anchor in bs(driver.page_source, "lxml").find_all("a", href=True):
            href: str = anchor['href']
            if href.endswith(".zip"):
                path = urlparse(href).path
                fname = unquote(path.split('/')[-1])
                fname = fname[:-4] 
                filenames.append((fname, href))

        print("Files to be downloaded:")
        for fname, _ in filenames:
            print(fname)
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            for fname, href in filenames:
                executor.submit(download_and_extract, href, fname)

    except Exception as e:
        print("Error occurred:", e)
    finally:
        driver.quit()
        if not os.listdir("zips"):
            rmdir("zips")

    # Remove duplicates, rename files, and clean up as necessary
    remove_duplicates_and_move(nyc_data_directory)
    rename_files(nyc_data_directory)

    remove_duplicates_and_move(jersey_city_directory)
    rename_files(jersey_city_directory)

    clean_up_files(nyc_data_directory)
    clean_up_files(jersey_city_directory)