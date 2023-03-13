# %%|
from big_querry_utils import BigQueryManager

# %%
bq_manager = BigQueryManager(dataset_id="fuel_qld", location="EU")
ext_tables = [("external_fuel_week", "week"), ("external_fuel_month", "month")]
bq_manager.create_new_ext_tables(bucket_name="230311fire", new_ext_tables=ext_tables)
bq_manager.check_tables_exist([table_name for table_name, _ in ext_tables])


# %%
