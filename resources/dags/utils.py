import pandas as pd
import sqlalchemy as sa
import requests

from configparser import ConfigParser


def request_data(url, key, topic):
    """
    En este caso particular la api nos explicita el link completo,
    de lo contrario se arma con la url base + endpoint + parámetro:
    """

    uri = str(url + key + f"&q={topic}")

    # Obtenemos datos haciendo un GET usando el método get de la librería
    resp = requests.get(uri)

    # Tenemos una lista de diccionario
    results = resp.json()["results"]

    # Entonces, podemos crear un DataFrame
    return pd.DataFrame(results)


def load_data(dataframe, schema, table_name, connection, dictionary_types):
    """
    Carga la info desde la API en un DataFrame, y éste a un archivo SQL.
    """
    dataframe.to_sql(schema=schema, name=f"stage_{table_name}", con=connection, if_exists='append', method='multi',
                     index=False,
                     dtype=dictionary_types)


def filter_data(df_request):
    """
    Filtra las columnas relevantes para la ETL.
    """
    return df_request[["article_id", "pubDate", "title", "creator", "category", "country", "language",
                       "link", "description"]].copy()


def build_conn_string(config_path, config_section):
    """
    Construye la cadena de conexión a la base de datos
    a partir de un archivo de configuración.
    """

    # Para leer el archivo de configuración
    parser = ConfigParser()
    parser.read(config_path)

    # Parámetros de conexión a Redshift
    config = parser[config_section]
    host = config['host']
    port = config['port']
    dbname = config['dbname']
    username = config['username']
    pwd = config['pwd']

    # Se construye la cadena de conexión
    conn_string = f'redshift+psycopg2://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'

    return conn_string


def connect_to_db(conn_string):
    """
    Crea una conexión a la base de datos.
    """
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine


def create_table(schema, table_name, script_name, conn):
    """
    Crea la tabla para la base de datos.
    """
    file = open(script_name, mode="r")
    query = file.read().format(schema=schema, table_name=table_name)
    file.close()
    conn.execute(query)


def clean_duplicates(script_name, table_name, conn):
    """
    Limpia los duplicados de la tabla.
    """
    file = open(script_name, "r")
    clean_query = file.read().format(table_name=table_name)
    conn.execute(clean_query)
