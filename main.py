import requests
import json
import pandas as pd

import os

# Para leer el archivo config.ini :
from configparser import ConfigParser

from pathlib import Path

# Instanciar la clase ConfigParser y lo asigno en una variable:
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
print(results)

# Entonces, podemos crear un DataFrame
df = pd.DataFrame(results)
# print(df.head(5))

print(df["title"], ["creator"])

# Cómo selecciono mostrar solo algunas columnas de results ???

