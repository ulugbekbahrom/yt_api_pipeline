from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from dwh.dwh import staging_table, core_table
from dataquality.soda import yt_elt_dq_checks
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

local_tz = pendulum.timezone("Asia/Tashkent")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "ulugbek.dae@gmail.com",
    # "retries": 1,
    # "retry_delay": timedelta(hours=1),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz),
}

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",
    catchup=False
) as dag_produce:

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db", 
        trigger_dag_id="update_db",
    )

    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into staging and core schemas",
    schedule=None,
    catchup=False
) as dag:
    
    update_staging = staging_table()
    update_core = core_table()

    trigger_dq_checks = TriggerDagRunOperator(
        task_id="trigger_dq_checks", 
        trigger_dag_id="dq_checks",
    )

    update_staging >> update_core >> trigger_dq_checks

with DAG(
    dag_id="dq_checks",
    default_args=default_args,
    description="Data Quality Checks for staging and core schemas",
    schedule=None,
    catchup=False
) as dag_dq_checks:
    
    soda_val_staging = yt_elt_dq_checks(staging_schema)
    soda_val_core = yt_elt_dq_checks(core_schema)

    soda_val_staging >> soda_val_core