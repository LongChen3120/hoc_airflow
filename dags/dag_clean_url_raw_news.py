
import datetime
import airflow
import airflow.operators
import airflow.operators.bash
import airflow.operators.python
from pytz import timezone


local_tz = timezone("Asia/Ho_Chi_Minh")

# dinh nghia dag
default_args = {
    "owner" : "Long2kVipProTN",
    "depends_on_past" : False,
    "retries" : 1,
    "retry_delay" : datetime.timedelta(minutes=5)
}

my_dag = airflow.DAG(
    dag_id="dag_clean_url_raw_news",
    default_args=default_args,
    description="dag chay project loai bo ban ghi cu trong DB",
    start_date=datetime.datetime(2024, 8, 2, tzinfo=local_tz),
    schedule="45 23 * * 0", # phut:45 gio:23 ngay:all thang:all ngay_trong_tuan:0(chu nhat)
    catchup=False
)

def run_project():
    import sys
    sys.path.append("/home/data_mount/2024/project_etl/clean/clean_url_news_raw")
    import main
    main.main()

task = airflow.operators.python.PythonOperator(
    task_id="run_main",
    python_callable=run_project,
    dag=my_dag
)