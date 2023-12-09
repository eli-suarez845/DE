import utils as ut
import sqlalchemy_redshift as sar
import sqlalchemy as sa
import pandas as pd

from psycopg2.extensions import register_adapter
from psycopg2.extras import Json
from airflow.utils.email import send_email


def create_tables(script, schema, table_name, conn_string):
    """ Se conecta a la base de datos y crea la tabla.
    """
    conn, engine = ut.connect_to_db(conn_string)
    ut.create_table(schema, table_name, script, conn)
    conn.close()


def load_data(url, key, topic, conn_string, schema, table_name):
    """
    Crea el dataframe, lo filtra y lo convierte en archivo Json
    especificando los tipos de datos y carga la tabla a la base de datos.
    """
    df = ut.request_data(url, key, topic)
    df_tabla = ut.filter_data(df)
    conn, engine = ut.connect_to_db(conn_string)

    register_adapter(list, Json)
    # Y se especifican los dtypes requeridos
    dict_types = {
        'article_id': sa.types.VARCHAR(),
        'pubDate': sa.types.DATE(),
        'title': sa.types.VARCHAR(),
        'creator': sar.dialect.SUPER(),
        'category': sar.dialect.SUPER(),
        'country': sar.dialect.SUPER(),
        'language': sa.types.VARCHAR(),
        'link': sa.types.VARCHAR(),
        'description': sa.types.VARCHAR()
    }

    ut.load_data(df_tabla, schema, table_name, conn, dict_types)
    conn.close()


def clean_duplicates(script_name, table_name, conn_string):
    """
    Limpia los duplicados de la tabla.
    """
    conn, engine = ut.connect_to_db(conn_string)
    ut.clean_duplicates(script_name, table_name, conn)
    conn.close()


def send_success_status_email(context):
    """
    Mediante el context, obtiene el status del proceso creando el cuerpo del mail con las variables necesarias
    y enviándolo al correo especificado.
    """
    task_status = context['task_instance'].current_state()

    subject = f"Airflow Task- {context['task_instance'].task_id}: {task_status}"
    body = f"<html> <head> <body><br>  Hi Dev, <br> The task <b> {context['task_instance'].task_id} " \
           f"</b> finished with status: <b>{task_status}</b> <br>" \
           f"<br> Task execution date: {context['execution_date']} " \
           f"<br> <p>Log URL: {context['task_instance'].log_url} " \
           f"<br> Kind regards<br> Dev Team </p></body> </head> </html>"

    to_email = ["elisasuarezmoreira@gmail.com"]  # Replace with the primary recipient email address

    send_email(to=to_email, subject=subject, html_content=body)


def send_failure_status_email(context):
    """
    Si el proceso falla en conectarse a la API y realizar la extracción,
    envía una alerta vía mail.
    """
    task_instance = context['task_instance']
    task_status = task_instance.current_state()

    subject = f"Airflow Task- {context['task_instance'].task_id}: {task_status}"
    body = f"<html> <head> <body><br>  Hi Dev, <br> The task <b> {context['task_instance'].task_id} " \
           f"</b> finished with status: <b>{task_status}</b> <br>" \
           f"<br> Task execution date: {context['execution_date']} " \
           f"<br> <p>Log URL: {context['task_instance'].log_url}<br><br> " \
           f"NOTE: Please resolve the error so as the UPSTREAM tasks can run, until then it's blocked. <br>" \
           f"<br> Regards<br> Dev Team</p></body> </head> </html>"

    to_email = ["elisasuarezmoreira@gmail.com"]  # Replace with the primary recipient email address

    send_email(to=to_email, subject=subject, html_content=body)


def send_retry_mail(context):
    """
        Envía una alerta vía mail informando los reintentos.
        """
    task_status = context['task_instance'].current_state()

    subject = f"Retrying task- {context['task_instance'].task_id}: {task_status}"
    body = f"<html> <head> <body><br>  Hi Dev, <br> The task <b> {context['task_instance'].task_id} " \
           f"</b> Task is up for retry: <b>{task_status}</b> <br>" \
           f"<br> Task execution date: {context['execution_date']} " \
           f"<br> <p>Log URL: {context['task_instance'].log_url} <br> Kind regards<br> Dev Team </p></body> </head> </html>"

    to_email = ["elisasuarezmoreira@gmail.com"]  # Replace with the primary recipient email address

    send_email(to=to_email, subject=subject, html_content=body)
