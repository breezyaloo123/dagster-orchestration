"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""

from dagster import AssetSelection,DefaultScheduleStatus,ScheduleDefinition


sales_schedule = ScheduleDefinition(
    name="sales_schedule",
    target=AssetSelection.groups("etl_schedule"),
    #Run this pipeline at this moment
    cron_schedule="40 18 * * *",
    default_status=DefaultScheduleStatus.RUNNING
)