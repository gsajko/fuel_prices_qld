# %%
from big_querry_utils import GcsUploader

# %%
project_dir = "/home/sajo/fuel_prices_qld/"
dataset_name = "fuel_qld"
bucket_name = "230314fuelprice"
location_region = "EU"

# %%
uploader = GcsUploader(project_dir, bucket_name, location_region, dataset_name)
uploader.save_to_gcs(
    src_folder="data/data/month",
    file="2018-12-queensland-fuel-prices-december-2018.csv",
    dest_folder="month",
)
# %%
