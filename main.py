# Instalamos librerías:
# Packages instalados: sqlalchemy-redshift, pandas

# Importamos librerías:
from configparser import ConfigParser
import pandas as pd
import sqlalchemy as sa

# Definimos funciones:

def build_conn_string(config_path, config_section):
    """
    Construye la cadena de conexión a la base de datos
    a partir de un archivo de configuración.
    """

    # Lee el archivo de configuración
    parser = ConfigParser()
    parser.read(config_path)

    # Lee la sección de configuración de PostgreSQL
    config = parser[config_section]
    host = config['host']
    port = config['port']
    dbname = config['dbname']
    username = config['username']
    pwd = config['pwd']

    # Construye la cadena de conexión
    conn_string = f'postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'

    return conn_string

# ----------------------------

def connect_to_db(conn_string):
    """
    Crea una conexión a la base de datos.
    """
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine


# Conexión a Redshift
config_dir = "venv/config/config.ini"
conn_string = build_conn_string(config_dir, "redshift")
conn, engine = connect_to_db(conn_string)

