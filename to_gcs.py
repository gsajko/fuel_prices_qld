# %%
import glob
import os
import shutil

import pandas as pd
from google.api_core.exceptions import Conflict, PreconditionFailed
from google.cloud import storage

# Set the absolute path to the key file
key_file_path = "/home/sajo/key.json"
# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path


class GcsUploader:
    def __init__(self, project_dir, bucket_name, location_region):
        self.project_dir = project_dir
        self.bucket_name = bucket_name
        self.location_region = location_region

    def create_bucket(self):
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.bucket_name)
            bucket = storage_client.create_bucket(bucket, location=self.location_region)
            print("Bucket {} created".format(bucket.name))
        except Conflict:
            print("Bucket {} already exists".format(self.bucket_name))

    def upload_blob(self, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        generation_match_precondition = 0
        blob.upload_from_filename(
            source_file_name, if_generation_match=generation_match_precondition
        )
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")

    def save_to_gcs(self, src_folder, file, dest_folder):
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
            dest_file_path = os.path.join(self.project_dir, "tmp", dest_file_name)
            df.to_parquet(dest_file_path)
            # upload to GCS
            dest_blob_name = f"{dest_folder}/{dest_file_name}"
            self.upload_blob(dest_file_path, dest_blob_name)
            # remove tmp file
            os.remove(dest_file_path)
        except PreconditionFailed:
            print(f"File {file} already exists in {dest_folder} folder")

    def upload_from_folders(self, folders):
        for folder in folders:
            src_folder = os.path.join(self.project_dir, "data/data", folder)
            os.chdir(src_folder)
            extension = "csv"
            all_filenames = [i for i in glob.glob("*.{}".format(extension))]
            # create tmp folder
            os.makedirs(os.path.join(self.project_dir, "tmp"), exist_ok=True)
            for file in all_filenames:
                print(f"uploading: {file}")
                self.save_to_gcs(src_folder=src_folder, file=file, dest_folder=folder)

        # Remove all files and subdirectories inside tmp directory
        shutil.rmtree(os.path.join(self.project_dir, "tmp"))


# %%
project_dir = "/home/sajo/fuel_prices_qld/"
bucket_name = "230313_fuelprice"
location_region = "EU"

uploader = GcsUploader(project_dir, bucket_name, location_region)
uploader.create_bucket()
uploader.upload_from_folders(["week", "month"])


# %%
