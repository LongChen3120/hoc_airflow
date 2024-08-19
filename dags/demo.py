from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pytz import timezone


local_tz = timezone('Asia/Ho_Chi_Minh')

# Định nghĩa DAG
default_args = {
    'owner': 'Nguyen Hai Long',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

my_dag = DAG(
    dag_id='demo',
    default_args=default_args,
    description='v1',
    start_date=datetime(2024, 8, 1, tzinfo=local_tz),
    schedule='0,30 * * * *',
    catchup=False
)


def run_crawl_detail_news():
    import sys
    sys.path.append("/home/data_mount/2024/crawl_news/scr")
    import main
    try:
        main.crawl_detail_news()
    except Exception as e:
        print(f"Run main.crawl_detail_news() exception:\n{e}")


task_3 = PythonOperator(
    task_id="Run_crawl_detail_news",
    python_callable=run_crawl_detail_news,
    dag=my_dag
)

task_3