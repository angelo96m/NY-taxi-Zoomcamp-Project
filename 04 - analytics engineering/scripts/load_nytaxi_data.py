import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time

# ==============================
# CONFIG
# ==============================

PROJECT_ID = "datazoomcamp-486517"
BUCKET_NAME = "nytaxi_bucket_zoomcamp"
DESTINATION_FOLDER = "nytaxi-data"

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv"

YEARS = ["2019"]
MONTHS = [f"{i:02d}" for i in range(1, 13)]

DOWNLOAD_DIR = "downloads"
CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

client = storage.Client(project=PROJECT_ID)
bucket = client.bucket(BUCKET_NAME)

# ==============================
# DOWNLOAD
# ==============================

def download_file(year_month):
    year, month = year_month
    filename = f"FHV_tripdata_{year}-{month}.csv.gz"
    url = f"{BASE_URL}/{filename}"
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


# ==============================
# UPLOAD
# ==============================

def upload_to_gcs(file_path, max_retries=3):
    blob_name = f"{DESTINATION_FOLDER}/{os.path.basename(file_path)}"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            return
        except Exception as e:
            print(f"Upload failed: {e}")
            time.sleep(5)

    print(f"Giving up on {file_path}")


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":

    # Generate (year, month) combinations
    year_month_list = [(year, month) for year in YEARS for month in MONTHS]

    # Download files
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, year_month_list))

    # Upload files
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))

    print("All files processed successfully.")