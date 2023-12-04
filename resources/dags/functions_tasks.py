import utils as ut
import sqlalchemy_redshift as sar
import sqlalchemy as sa
import pandas as pd

from psycopg2.extensions import register_adapter
from psycopg2.extras import Json


def create_tables(script, schema, table_name, conn_string):
    conn, engine = ut.connect_to_db(conn_string)
    ut.create_table(schema, table_name, script, conn)
    conn.close()


def load_data(url, key, topic, conn_string, schema, table_name):
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
    conn, engine = ut.connect_to_db(conn_string)
    ut.clean_duplicates(script_name, table_name, conn)
    conn.close()


