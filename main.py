
# Python Packages instalados: sqlalchemy-redshift, psycopg2

# Se importa librerías:
from configparser import ConfigParser
import sqlalchemy as sa

# Se define funciones:

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


# ----------------------------

config = ConfigParser()

# base_dir = ("C:\Users\Elisa\Desktop\CoderHouse\Data engineering")

# os.chdir(base_dir)

config_dir = "venv/config/config.ini"
config.read(config_dir)  # el contenido del archivo queda en ese config

# Chequeamos que la sección y la variable declarada se lean correctamente:
# print(config.sections())
key = config["API_KEY_NEWSDATAIO"]["key"]

# En este caso particular la api nos da el link armado,
# de lo contrario armar url base + endpoint + parámetros:
url = str("https://newsdata.io/api/1/news?apikey=" + key + "&q=ucrania")
# print(url)

# Obtenemos datos haciendo un GET usando el método get de la librería
resp = requests.get(url)

# Podemos ver el contenido del archivo json:
# print(resp.json())

# Tenemos una lista de diccionario
results = resp.json()["results"]
#print(results)

# Entonces, podemos crear un DataFrame
df = pd.DataFrame(results)
# print(df.head(5))

print(df["title"],["creator"])

# ----------------------------

# Se establece la conexión a Redshift
config_dir = "venv/config/config.ini"
conn_string = build_conn_string(config_dir, "redshift")
conn, engine = connect_to_db(conn_string)

schema = "elisasuaezmoreira_coderhouse"


# Se define script una tabla
create_table_query = """
CREATE TABLE IF NOT EXISTS {schema}.PrimerEntregable2(
    article_id VARCHAR(50) PRIMARY KEY distkey,
    title VARCHAR(100),
    creator VARCHAR(50),
    link VARCHAR(100),
    pubDate DATE,
    language VARCHAR(50),
    category VARCHAR(50),
    country VARCHAR(50),
    description VARCHAR(500)
)
SORTKEY (pubDate);
"""


# Se ejecuta
conn.execute(create_table_query)







