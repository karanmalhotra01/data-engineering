import calendar
import dlt
from dlt.common import pendulum
from dlt.common.data_writers import TDataItemFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItems
from dlt.pipeline.pipeline import Pipeline
from dlt.destinations.adapters import athena_partition, athena_adapter

# As this pipeline can be run as standalone script or as part of the tests, we need to handle the import differently.
try:
    from .mongodb import mongodb, mongodb_collection  # type: ignore
except ImportError:
    from mongodb import mongodb, mongodb_collection

import yaml

from mongodb_pipeline import full_refresh


def run_dlt_batches(collection_name: str,
                    incremental_field: str,
                    initial_date: str,
                    end_date: str):

    # Convert to pendulum date objects
    start = pendulum.parse(initial_date)
    end = pendulum.parse(end_date)

    current_date = start

    while current_date <= end:
        # Last day of the current month
        last_day = calendar.monthrange(current_date.year, current_date.month)[1]
        month_end = current_date.replace(day=last_day)

        # Do not exceed full end date
        if month_end > end:
            month_end = end

        start_str = current_date.to_date_string()
        end_str = month_end.to_date_string()

        print(f"\n🚀 Running DLT Full Refresh for {collection_name}")
        print(f"📅 Batch: {start_str} → {end_str}")

        try:
            # Call your DLT code
            full_refresh(
                collection_name=collection_name,
                incremental_field=incremental_field,
                initial_date=start_str,
                end_date=end_str
            )
        except Exception as e:
            print(f"❌ Error while processing batch {start_str} → {end_str}: {e}")
            break

        # Move to next month
        next_month = current_date.month + 1
        next_year = current_date.year

        if next_month > 12:
            next_month = 1
            next_year += 1

        current_date = current_date.replace(year=next_year, month=next_month, day=1)


def main():
    """Reads YAML and runs DLT batches for each job."""

    with open("config_mongo_batches.yml", "r") as f:
        config = yaml.safe_load(f)

    for job in config["jobs"]:
        collection_name = job["collection_name"]
        incremental_field = job["incremental_field"]
        initial_date = job["initial_date"]
        end_date = job["end_date"]

        print(f"\n==============================")
        print(f"🔥 Processing JOB: {collection_name}")
        print(f"==============================")

        run_dlt_batches(
            collection_name=collection_name,
            incremental_field=incremental_field,
            initial_date=initial_date,
            end_date=end_date
        )


if __name__ == "__main__":
    main()