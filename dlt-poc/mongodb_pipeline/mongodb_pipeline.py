import dlt
from dlt.common import pendulum
from dlt.common.data_writers import TDataItemFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItems
from dlt.pipeline.pipeline import Pipeline
from dlt.destinations.adapters import athena_partition, athena_adapter
import sys
import os

# Add parent directory to sys.path for shared modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from slack_notifier import send_slack_webhook

# As this pipeline can be run as standalone script or as part of the tests, we need to handle the import differently.
try:
    from .mongodb import mongodb, mongodb_collection  # type: ignore
except ImportError:
    from mongodb import mongodb, mongodb_collection

import yaml




def full_load(collection_name: str):

    try:
        pipeline = dlt.pipeline(
            pipeline_name=collection_name,
            destination='athena',
            dataset_name="dlt_distribution",
        )

        # Direct collection loading - no incremental
        source = mongodb().with_resources(collection_name)
        info = pipeline.run(source, write_disposition="replace")
        # print(f"Full load completed for {collection_name}")
    except Exception as e:
        error_msg = f"[❌] Failed job: Full_Load - {collection_name} | Error: {str(e)}"
        send_slack_webhook(error_msg)



def incremental_load(collection_name: str, incremental_field: str, initial_date: str):

    try:
        pipeline = dlt.pipeline(
            pipeline_name=collection_name,
            destination='athena',
            dataset_name="dlt_distribution"
        )


        source = mongodb(
            incremental=dlt.sources.incremental(
                incremental_field,
                initial_value=pendulum.parse(initial_date)
                )).with_resources(collection_name)

        source = athena_adapter(source, partition=[athena_partition.day(incremental_field)])

        info = pipeline.run(source, write_disposition="merge")
    except Exception as e:
        error_msg = f"[❌] Failed job: Incremental_Load - {collection_name} | Error: {str(e)}"
        send_slack_webhook(error_msg)

def full_refresh(collection_name: str, incremental_field: str, initial_date:str, end_date:str):
    pipeline = dlt.pipeline(
        pipeline_name=collection_name,
        destination='athena',
        dataset_name="dlt_distribution"
    )


    source = mongodb(
        incremental=dlt.sources.incremental(
            incremental_field,
            initial_value=pendulum.parse(initial_date),
            end_value=pendulum.parse(end_date)
            )
            ).with_resources(collection_name)

    source = athena_adapter(source, partition=[athena_partition.day(incremental_field)])

    info = pipeline.run(source, write_disposition="merge")
    print(f"Incremental load completed for {collection_name}")

    #info = pipeline.run(source, write_disposition="replace") # we need perform this in case of reload of a table
    #info = pipeline.run(source, write_disposition="append") # for hive tables
    # info = pipeline.run(source, write_disposition="merge")  # for iceberg tables

def main():
    send_slack_webhook(f"✅ *MongoDB DISTRIBUTION job Started*")

    with open("config_mongo.yml", "r") as f:
        config = yaml.safe_load(f)

    for job in config['jobs']:
        collection_name = job.get('collection_name')
        load_strategy = job.get('load_strategy')
        incremental_field = job.get('incremental_field',None)
        initial_date = job.get("initial_date",None)

        if load_strategy == "full":
            full_load(collection_name)
        else:
            incremental_load(collection_name, incremental_field,initial_date)
    send_slack_webhook(f"✅ *MongoDB DISTRIBUTION job Completed*")

if __name__ == "__main__":
    main()