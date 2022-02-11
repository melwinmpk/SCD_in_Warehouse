from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

ROOT_PATH = "/home/saif/SCD_in_Warehouse"
default_args = {
    'owner': 'melwin',
    'depends_on_past': False,
    'email': ['melwin@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    }

dag1 = DAG(
    'SCD_IN_WAREHOUSE',
    default_args=default_args,
    description='SCD TRIGGER',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
)

t1 = BashOperator(
task_id='TRIGGER_SQOOP_JOB',
bash_command="""
sqoop-job --exec loadcustomers
sqoop-job --exec loadsalesorderheader
sqoop-job --exec loadsalesorderheader
sqoop-job --exec loadproducts
sqoop-job --exec loadcreditcard
""",
dag=dag1
)

t2 = BashOperator(
task_id='Execute_SCD1',
bash_command=f"""
        hive -e "
        source {ROOT_PATH}/hive/creditcard_scd1.hql;
        "
""",
dag=dag1
)

t3 = BashOperator(
task_id='Execute_SCD2',
bash_command=f"""
        hive -e "
        source {ROOT_PATH}/hive/customer_scd2.hql
        "
""",
dag=dag1
)

t4 = BashOperator(
task_id='Execute_SCD4',
bash_command=f"""
        python3 {ROOT_PATH}/spark/scd4.py
""",
dag=dag1
)

start = DummyOperator(task_id='start', dag=dag1)
end = DummyOperator(task_id='end', dag=dag1)

start >> t1 >> t2 >> t3 >> t4 >> end