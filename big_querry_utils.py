# %%
import glob
import os
import shutil

import pandas as pd
from google.api_core.exceptions import Conflict, PreconditionFailed
from google.cloud import bigquery, storage

# Set the absolute path to the key file
key_file_path = "/home/sajo/key.json"
# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path


class BigQueryManager:
    def __init__(self, dataset_id, location):
        self.client = bigquery.Client()
        self.dataset_ref = self._create_dataset(dataset_id, location)

    def _create_dataset(self, dataset_id, location):
        print("Creating dataset")
        dataset = bigquery.Dataset(self.client.dataset(dataset_id))
        dataset.location = location
        try:
            # Create the dataset
            dataset = self.client.create_dataset(dataset)
            # Print the dataset details
            print(f"Dataset {dataset.project}.{dataset.dataset_id} was created.")
        except Conflict:
            print(f"Dataset {dataset.project}.{dataset.dataset_id} already exists.")
        return dataset

    def create_ext_table_from_parquet(self, bucket, path, table_name):
        uri_template = f"gs://{bucket}/{path}/*.parquet"
        table_name_full = f"{self.dataset_ref.dataset_id}.{table_name}"

        sql = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{table_name_full}`
        OPTIONS (
            format = 'PARQUET',
            uris = ['{uri_template}']
        );
        """
        # Run the SQL statement
        query_job = self.client.query(sql)

        # Wait for the query to complete
        query_job.result()

    def create_new_ext_tables(self, bucket_name, new_ext_tables):
        for table_name, path in new_ext_tables:
            self.create_ext_table_from_parquet(
                bucket=bucket_name, path=path, table_name=table_name
            )

    def check_tables_exist(self, table_names):
        tables = self.client.list_tables(self.dataset_ref)
        existing_table_names = [table.full_table_id.split(".")[-1] for table in tables]
        for table_name in table_names:
            assert table_name in existing_table_names
            print(f"Table {table_name} created successfully")


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
            # lower column names
            df.columns = [x.lower() for x in df.columns]
            # set type of date columns
            df["transactiondateutc"] = pd.to_datetime(df["transactiondateutc"])

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
        os.makedirs(os.path.join(self.project_dir, "tmp"), exist_ok=True)
        for folder in folders:
            src_folder = os.path.join(self.project_dir, "data/data", folder)
            os.chdir(src_folder)
            extension = "csv"
            all_filenames = [i for i in glob.glob("*.{}".format(extension))]
            # create tmp folder
            for file in all_filenames:
                print(f"uploading: {file}")
                self.save_to_gcs(src_folder=src_folder, file=file, dest_folder=folder)
        # Remove all files and subdirectories inside tmp directory
        shutil.rmtree(os.path.join(self.project_dir, "tmp"))
