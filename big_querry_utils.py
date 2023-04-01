import glob
import os
import shutil
import subprocess
from typing import List

import pandas as pd
from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage

# Set the absolute path to the key file
key_file_path = "/home/sajo/key.json"
# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path


class BigQueryManager:
    def __init__(self, dataset_id: str, location: str) -> None:
        self.client = bigquery.Client()
        self.dataset_ref = self._create_dataset(dataset_id, location)

    def _create_dataset(self, dataset_id: str, location: str) -> bigquery.Dataset:
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

    def create_ext_table_from_parquet(self, bucket: str, path: str, table_name: str):
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

    def create_new_ext_tables(self, bucket_name: str, new_ext_tables: list) -> None:
        for table_name, path in new_ext_tables:
            self.create_ext_table_from_parquet(
                bucket=bucket_name, path=path, table_name=table_name
            )

    def check_tables_exist(self, table_names: list) -> None:
        tables = self.client.list_tables(self.dataset_ref)
        existing_table_names = [table.full_table_id.split(".")[-1] for table in tables]
        for table_name in table_names:
            assert table_name in existing_table_names
            print(f"Table {table_name} created successfully")


class GcsUploader:
    def __init__(
        self,
        project_dir: str,
        bucket_name: str,
        location_region: str,
        dataset_name: str,
    ):
        self.project_dir = project_dir
        self.bucket_name = bucket_name
        self.location_region = location_region
        self.dataset_name = dataset_name

    def create_bucket(self) -> None:
        try:
            storage_client: storage.Client = storage.Client()
            bucket: storage.Bucket = storage_client.bucket(self.bucket_name)
            bucket = storage_client.create_bucket(bucket, location=self.location_region)
            print("Bucket {} created".format(bucket.name))
        except Conflict:
            print("Bucket {} already exists".format(self.bucket_name))

    def upload_blob(self, source_file_name: str, destination_blob_name: str):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        if blob.exists() and bucket.get_blob(
            destination_blob_name
        ).size == os.path.getsize(source_file_name):
            print(f"File {source_file_name} already exists.")
            return "skip"
        else:
            blob.upload_from_filename(source_file_name)
            print(f"File {source_file_name} uploaded to {destination_blob_name}.")
            return "success"

    def save_to_gcs(self, src_folder: str, file: str, dest_folder: str):
        date_column = ""
        # Set the absolute path to the source file
        src_file_path = os.path.join(src_folder, file)
        if src_folder.split("/")[-1] == "month":
            date_column = "TransactionDateutc"

            def date_parser(x):
                return pd.to_datetime(x, format="%d/%m/%Y %H:%M")

        elif src_folder.split("/")[-1] == "week":
            date_column = "TransactionDateUtc"

            def date_parser(x):
                return pd.to_datetime(x, format="%Y-%m-%d %H:%M")

        else:
            raise ValueError("Wrong src_folder")
        try:
            df = pd.read_csv(
                src_file_path,
                parse_dates=[date_column],
                date_parser=date_parser,
            )
        except UnicodeDecodeError:
            df = pd.read_csv(
                src_file_path,
                parse_dates=[date_column],
                date_parser=date_parser,
                encoding="Windows-1252",
            )
        # save to tmp folder
        file_name = file.split(".")[0]
        # lower column names
        df.columns = [x.lower() for x in df.columns]
        dest_file_name = f"{file_name}.parquet"
        dest_file_path = os.path.join(self.project_dir, "tmp", dest_file_name)
        df.to_parquet(dest_file_path)
        # upload to GCS
        dest_blob_name = f"{dest_folder}/{dest_file_name}"
        results = self.upload_blob(dest_file_path, dest_blob_name)
        return results

    def upload_monthly_files(self, files: List[str], snapshot: bool = False) -> None:
        for file in files:
            print(f"uploading: {file}")
            src_folder: str = os.path.join(self.project_dir, "data/data/month")
            results: str = self.save_to_gcs(
                src_folder=src_folder, file=file, dest_folder="month"
            )
            if snapshot and results == "success":
                bq_manager: BigQueryManager = BigQueryManager(
                    dataset_id=self.dataset_name, location=self.location_region
                )
                bq_manager.create_ext_table_from_parquet(
                    bucket=self.bucket_name,
                    path="month",
                    table_name="external_fuel_month",
                )
                self.run_dbt_snapshot()

    def upload_weekly_files(self, files: List[str]) -> None:
        for file in files:
            print(f"uploading: {file}")
            src_folder: str = os.path.join(self.project_dir, "data/data/week")
            self.save_to_gcs(src_folder=src_folder, file=file, dest_folder="week")

    def run_dbt_snapshot(self) -> None:
        # TODO hardcoded paths
        DBT_PROFILES_DIR: str = "/home/sajo/fuel_prices_qld/dbt_fuel/config"
        DBT_PROJECT_DIR: str = "/home/sajo/fuel_prices_qld/dbt_fuel"
        dbt_command: str = (
            f"dbt snapshot --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        )
        try:
            result = subprocess.run(
                dbt_command, shell=True, check=True, capture_output=True, text=True
            )
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(e.stderr)
            print(e.stdout)

    def upload_from_folders(
        self, folders: List[str], snapshot: bool = False, **kwargs
    ) -> None:
        os.makedirs(os.path.join(self.project_dir, "tmp"), exist_ok=True)
        for folder in folders:
            src_folder = os.path.join(self.project_dir, "data/data", folder)
            os.chdir(src_folder)
            extension = "csv"
            all_filenames = [i for i in glob.glob("*.{}".format(extension))]
            # sort files
            all_filenames.sort()
            # snapshot
            if folder == "month":
                self.upload_monthly_files(
                    files=all_filenames, snapshot=snapshot, **kwargs
                )
            if folder == "week":
                self.upload_weekly_files(files=all_filenames)
        shutil.rmtree(os.path.join(self.project_dir, "tmp"))
