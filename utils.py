import pandas as pd
import sqlalchemy as sa
from configparser import ConfigParser


def load_data(dataframe, schema, table_name, connection, dictionary_types):
    dataframe.to_sql(schema=schema, name=table_name, con=connection, if_exists='append', method='multi', index=False,
                     dtype=dictionary_types)


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
