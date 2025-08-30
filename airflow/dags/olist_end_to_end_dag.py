from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

try:
    from etl.download_kaggle import download_kaggle_dataset
except ModuleNotFoundError:
    import sys
    sys.path.append("/opt/airflow") 
    from etl.download_kaggle import download_kaggle_dataset

with DAG(
    dag_id="olist_end_to_end",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["olist"],
    default_args={"retries": 1},
) as dag:

    kaggle_dl = PythonOperator(
        task_id="kaggle_download",
        python_callable=download_kaggle_dataset,
    )

    etl = BashOperator(
        task_id="etl_load_raw",
        bash_command="python /opt/airflow/etl/load_to_postgres.py 2>&1",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt && dbt seed --profiles-dir profiles 2>&1",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir profiles 2>&1",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir profiles 2>&1",
    )

    kaggle_dl >> etl >> dbt_seed >> dbt_run >> dbt_test
