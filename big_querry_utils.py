# %%
import os

from google.cloud import bigquery

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
        except Exception as e:
            if type(e).__name__ == "Conflict":
                print(f"Dataset {dataset.project}.{dataset.dataset_id} already exists.")
            else:
                raise e
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
