import utils as ut
import functions_tasks as ft
import pendulum

from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from configparser import ConfigParser

schema = "elisasuaezmoreira_coderhouse"
table_name = "news"

config = ConfigParser()
config_dir = "/opt/airflow/config/config.ini"
config.read(config_dir)
conn_string = ut.build_conn_string(config_dir, "redshift")

key = config["API_KEY_NEWSDATAIO"]["key"]
url = "https://newsdata.io/api/1/news?apikey="
topic = "ucrania"

# Se configuran los argumentos por defecto para el DAG
default_args = {
    'owner': 'Elisa',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 12, 3, tz="UTC"),
    'email': ['elisasuarezmoreira@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'email_to': ['elisasuarezmoreira@gmail.com'],
    'on_failure_callback': ft.send_failure_status_email,
    'on_success_callback': ft.send_success_status_email,
    'on_retry_callback': ft.send_retry_mail
}

ingestion_dag = DAG(
    dag_id='ingestion_data',
    default_args=default_args,
    description='Ingesta de datos de API NewsDataIO',
    start_date=pendulum.datetime(2023, 12, 3, tz="UTC"),
    schedule_interval=timedelta(days=1),  # corre de manera diaria porque la API se actualiza de manera diaria
    catchup=False
)

table_creation = PythonOperator(
    task_id='table_creation',
    python_callable=ft.create_tables,
    op_kwargs={'script': '/opt/airflow/dags/sql/creation_table_script.sql', 'schema': schema,
               'table_name': table_name, 'conn_string': conn_string},
    dag=ingestion_dag,
)


load_data = PythonOperator(
    task_id='load_data',
    python_callable=ft.load_data,
    op_kwargs={'url': url, 'key': key, 'topic': topic, 'conn_string': conn_string, 'schema': schema,
               'table_name': table_name},
    dag=ingestion_dag,
)

clean_duplicates = PythonOperator(
    task_id='clean_duplicates',
    python_callable=ft.clean_duplicates,
    op_kwargs={'script_name': '/opt/airflow/dags/sql/clean_duplicates.sql', 'table_name': table_name,
               'conn_string': conn_string},
    dag=ingestion_dag,
)


table_creation >> load_data >> clean_duplicates
