import yaml
import os
import copy
import mssql_pipeline
import pendulum

def run_historical_batches():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, "config_mssql_backfill.yml")

    if not os.path.exists(config_file):
        print(f"❌ Config file {config_file} not found")
        return

    with open(config_file, 'r') as f:
        backfill_config = yaml.safe_load(f)

    jobs = backfill_config.get('jobs', [])
    if not jobs:
        print("⚠️ No jobs found in config_mssql_backfill.yml")
        return

    print(f"🔎 Found {len(jobs)} jobs in configuration")

    for job_idx, job in enumerate(jobs):
        table_name = job.get('table_name')
        database = job.get('database')

        # Check if backfill is configured for this job
        initial_value_str = job.get('initial_value')
        end_date_str = job.get('end_date')

        if not initial_value_str or not end_date_str:
            print(f"⏩ Skipping {database}.{table_name}: No backfill dates configured (initial_value/end_date empty)")
            continue

        chunk_days = job.get('chunk_days', 99)

        try:
            start_dt = pendulum.parse(str(initial_value_str)).naive()
            final_end_dt = pendulum.parse(str(end_date_str)).naive()
        except Exception as e:
            print(f"❌ Error parsing dates for {database}.{table_name}: {e}")
            continue

        print(f"\n🚀 STARTING BACKFILL FOR: {database}.{table_name}")
        print(f"   📅 Range: {start_dt.to_date_string()} -> {final_end_dt.to_date_string()} (Chunks: {chunk_days} days)")

        current_start = start_dt
        while current_start < final_end_dt:
            current_end = current_start.add(days=chunk_days)
            if current_end > final_end_dt:
                current_end = final_end_dt

            print(f"   🔄 Processing Batch: {current_start.to_date_string()} to {current_end.to_date_string()}")

            # Create a single-job config for isolation
            single_job_config = {
                'jobs': [copy.deepcopy(job)]
            }

            # Update the specific job instance with current chunk dates
            job_in_batch = single_job_config['jobs'][0]
            job_in_batch['load_strategy'] = 'incr' # Enforce incremental
            job_in_batch['initial_value'] = current_start.to_date_string()
            job_in_batch['end_date'] = current_end.to_date_string()

            # Execute pipeline for this chunk
            try:
                mssql_pipeline.run_pipeline(manual_config=single_job_config)
                print(f"   ✅ Batch Success")
            except Exception as e:
                print(f"   ❌ Batch Failed: {str(e)}")
                # Decide: break loop or continue? Breaking to be safe for this table.
                break

            current_start = current_end

        print(f"🏁 Completed backfill for {database}.{table_name}\n")

    print("🎉 All Backfill Jobs Processed")

if __name__ == "__main__":
    run_historical_batches()