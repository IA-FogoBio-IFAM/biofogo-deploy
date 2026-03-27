import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

WMS_URL = os.getenv("WMS_URL", "https://panorama.sipam.gov.br/geoserver/painel_do_fogo/wms")
WFS_URL = os.getenv("WFS_URL", "https://panorama.sipam.gov.br/geoserver/painel_do_fogo/wfs")

PG_SCHEMA = os.getenv("PG_SCHEMA", "sipam")

PG_DSN = (
    f"host={os.getenv('PG_HOST', 'localhost')} "
    f"port=5432 "
    f"dbname={os.getenv('PG_DBNAME')} "
    f"user={os.getenv('PG_USER')} "
    f"password={os.getenv('PG_PASSWORD')}"
)


def get_conn():
    """Retorna uma conexão psycopg2."""
    return psycopg2.connect(PG_DSN, cursor_factory=RealDictCursor)


def ensure_schema():
    """Garante que o schema e a extensão PostGIS existem."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA}')
            cur.execute('CREATE EXTENSION IF NOT EXISTS postgis')
        conn.commit()
