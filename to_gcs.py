# %%
import glob
import os
import shutil

import pandas as pd
from google.api_core.exceptions import Conflict, PreconditionFailed
from google.cloud import storage

# Set the absolute path to the key file
key_file_path = "/home/sajo/key.json"

# Set the absolute path to the data directory
project_dir = "/home/sajo/fuel_prices_qld/"

bucket_name = "230311_fuelprice"
location_region = "EU"

# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path
# create tmp folder
os.makedirs(os.path.join(project_dir, "tmp"), exist_ok=True)
# %%
def create_bucket(bucket_name, location):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        bucket.location = location
        bucket = storage_client.create_bucket(bucket, location=location)
        print("Bucket {} created".format(bucket.name))
    except Conflict:
        print("Bucket {} already exists".format(bucket_name))


# %%
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    generation_match_precondition = 0
    blob.upload_from_filename(
        source_file_name, if_generation_match=generation_match_precondition
    )
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


def save_to_gcs(bucket_name, src_folder, file, dest_folder):
    try:
        # Set the absolute path to the source file
        src_file_path = os.path.join(src_folder, file)
        try:
            df = pd.read_csv(src_file_path)
        except UnicodeDecodeError:
            df = pd.read_csv(src_file_path, encoding="Windows-1252")
        # save to tmp folder
        file_name = file.split(".")[0]
        dest_file_name = f"{file_name}.parquet"
        dest_file_path = os.path.join(project_dir, "tmp", dest_file_name)
        df.to_parquet(dest_file_path)
        # upload to GCS
        dest_blob_name = f"{dest_folder}/{dest_file_name}"
        upload_blob(bucket_name, dest_file_path, dest_blob_name)
        # remove tmp file
        os.remove(dest_file_path)
    except PreconditionFailed:
        print(f"File {file} already exists in {dest_folder} folder")


# Loop through the source folders and files
def upload_from_folders(folders, bucket):
    for folder in folders:
        src_folder = os.path.join(project_dir, "data/data", folder)
        os.chdir(src_folder)
        extension = "csv"
        all_filenames = [i for i in glob.glob("*.{}".format(extension))]
        for file in all_filenames:
            print(f"uploading: {file}")
            save_to_gcs(
                bucket_name=bucket, src_folder=src_folder, file=file, dest_folder=folder
            )


folders = ["week", "month"]

create_bucket(bucket_name, location=location_region)
upload_from_folders(folders, bucket=bucket_name)

# Remove all files and subdirectories inside tmp directory
shutil.rmtree(os.path.join(project_dir, "tmp"))

# %%
