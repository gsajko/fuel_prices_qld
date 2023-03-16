# %%
from dagster._utils import file_relative_path
from dagster_dbt import dbt_cli_resource

# %%
print(file_relative_path(__file__, "../../dbt_project"))
# %%

DBT_PROJECT_DIR = file_relative_path(__file__, "../../dbt_project")
DBT_PROFILES_DIR = DBT_PROJECT_DIR + "/config"
dbt_local_resource = dbt_cli_resource.configured(
    {
        "profiles_dir": DBT_PROFILES_DIR,
        "project_dir": DBT_PROJECT_DIR,
        "target": "local",
    }
)
# %%
