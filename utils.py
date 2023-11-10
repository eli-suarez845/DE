import pandas as pd
import sqlalchemy as sa


def load_data(dataframe, table_name, connection, schema, dictionary_types):
    dataframe.to_sql(schema=schema, name=table_name, con=connection, if_exists='append', method='multi', index=False, dtype=dictionary_types)


def connect_to_db(conn_string):
    """
    Crea una conexión a la base de datos.
    """
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine
