from dagster import Definitions, define_asset_job, load_assets_from_modules

from my_dagster_project import assets
#from .assets import sales_schedule
from .schedules import sales_schedule

all_assets = load_assets_from_modules([assets])


laod_data_job = define_asset_job(name="load_data_job", selection="load_data")
cleaning_job = define_asset_job(name="cleaning_job", selection="cleaning_data")
send_data_job = define_asset_job(name="send_data_job", selection="send_data_SQLSERVER")



defs = Definitions(
    assets=all_assets,
    jobs=[laod_data_job,cleaning_job,send_data_job],
    schedules=[sales_schedule]
)
