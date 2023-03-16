# %%
from big_querry_utils import BigQueryManager, GcsUploader

# %%
project_dir = "/home/sajo/fuel_prices_qld/"
dataset_name = "fuel_qld"
bucket_name = "230314fuelprice"
location_region = "EU"
# %%
# upload data from repo to GCS bucket
uploader = GcsUploader(project_dir, bucket_name, location_region)
uploader.create_bucket()
# TODO create bucket with Terraform instead
uploader.upload_from_folders(
    folders=["week", "month"],
    snapshot=True,
    dataset_id=dataset_name,
    location=location_region,
    table_name="external_fuel_month",
    bucket_name=bucket_name,
)
# uploader.upload_from_folders(folders=["week", "month"])
# %%
# Create external tables
bq_manager = BigQueryManager(dataset_id=dataset_name, location=location_region)
ext_tables = [("external_fuel_week", "week"), ("external_fuel_month", "month")]
bq_manager.create_new_ext_tables(bucket_name=bucket_name, new_ext_tables=ext_tables)
bq_manager.check_tables_exist([table_name for table_name, _ in ext_tables])


# %%
# dbt setup
bq_manager._create_dataset("dbt_gsajko", location_region)
bq_manager._create_dataset("staging", location_region)
bq_manager._create_dataset("production", location_region)
# %%

# SQL
#
