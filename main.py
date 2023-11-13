# Python Packages instalados: sqlalchemy-redshift, psycopg2

# Se importan librerías:

import requests
import sqlalchemy as sa
import pandas as pd
import utils as ut

import sqlalchemy_redshift as sar
from psycopg2.extensions import register_adapter
from psycopg2.extras import Json

from configparser import ConfigParser

# ----------------------------

schema = "elisasuaezmoreira_coderhouse"
table_name = "news"

# Se accede a las credenciales del archivo config.ini:

config = ConfigParser()
config_dir = "venv/config/config.ini"
config.read(config_dir)

# Se chequea que la sección y la variable declarada se lean correctamente:
# print(config.sections())

key = config["API_KEY_NEWSDATAIO"]["key"]
tema = "ucrania"

# En este caso particular la api nos explicita el link completo,
# de lo contrario se arma con la url base + endpoint + parámetro:
url = str("https://newsdata.io/api/1/news?apikey=" + key + f"&q={tema}")
# print(url)

# Obtenemos datos haciendo un GET usando el método get de la librería
resp = requests.get(url)

# Podemos ver el contenido del archivo json:
# print(resp.json())

# Tenemos una lista de diccionario
results = resp.json()["results"]
# print(results)

# Entonces, podemos crear un DataFrame
df = pd.DataFrame(results)
# print(df.head(5))

# ----------------------------

# Se establece la conexión a Redshift
conn_string = ut.build_conn_string(config_dir, "redshift")
conn, engine = ut.connect_to_db(conn_string)

# ----------------------------

# Se define script de la tabla

ut.create_table(schema, table_name, "creation_table_script.sql", conn)

# Se crea un DataFrame con las columnas requeridas para la tabla
df_tabla = df[["article_id", "pubDate", "title", "creator", "category", "country", "language",
               "link", "description"]].copy()
# print(df_tabla.head(10))

# Se utiliza el PostgreSQL database adapter de Psycopg

register_adapter(list, Json)

# Y se especifican los dtypes requeridos
dict_types={
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

# Se escribe la data en la tabla de Redshift

ut.load_data(df_tabla, schema, table_name, conn, dict_types)

ut.clean_duplicates("clean_duplicates.sql", table_name, conn)

conn.close()





