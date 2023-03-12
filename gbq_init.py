# %%
import os

from google.cloud import bigquery

# %%
# Set the absolute path to the key file
key_file_path = "/home/sajo/key.json"
# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path
# create
# %%
# Set up the BigQuery client
client = bigquery.Client()

# create a dataset
def create_dataset(dataset_id="fuel_qld", location="EU"):
    dataset = bigquery.Dataset(client.dataset(dataset_id))
    dataset.location = location
    try:
        # Create the dataset
        dataset = client.create_dataset(dataset)
        # Print the dataset details
        print(f"Dataset {dataset.project}.{dataset.dataset_id} was created.")
    except Exception as e:
        if type(e).__name__ == "Conflict":
            print(f"Dataset {dataset.project}.{dataset.dataset_id} already exists.")
        else:
            raise e
    return dataset


dataset_ref = create_dataset("fuel_qld")
# %%
def create_ext_table_from_parq(bucket, path, dataset_name, table_name):
    uri_template = f"gs://{bucket}/{path}/*.parquet"
    table_name_full = f"{dataset_name}.{table_name}"

    sql = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{table_name_full}`
    OPTIONS (
        format = 'PARQUET',
        uris = ['{uri_template}']
    );
    """
    # Run the SQL statement
    query_job = client.query(sql)

    # Wait for the query to complete
    query_job.result()


# %%
# create new tables
bucket = "230311fire"
dataset_name = "fuel_qld"
new_ext_tables = [("external_fuel_week", "week"), ("external_fuel_month", "month")]
for table_name, path in new_ext_tables:
    create_ext_table_from_parq(bucket, path, dataset_name, table_name)

tables = client.list_tables(dataset_name)
table_names = [table.full_table_id.split(".")[-1] for table in tables]
# check if the table exists
for table_name, path in new_ext_tables:
    assert table_name in table_names
    print(f"Table {table_name} created successfully")

# TODO: assert if the tables are not empty
# TODO: refactor the code
