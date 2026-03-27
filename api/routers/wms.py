import json
import re
import xml.etree.ElementTree as ET
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from config import WMS_URL, WFS_URL, PG_SCHEMA, get_conn, ensure_schema

router = APIRouter(prefix="/wms", tags=["WMS"])


# ─── Schemas ──────────────────────────────────────────────────────────────────

class Camada(BaseModel):
    name: str
    title: str
    abstract: Optional[str] = None
    crs: list[str] = []


class ImportResult(BaseModel):
    layer: str
    table: str
    schema: str
    total_inserido: int
    status: str


# ─── Helpers ──────────────────────────────────────────────────────────────────

JAVA_ARRAY_RE = re.compile(r"^\[L[a-zA-Z.]+;@[0-9a-f]+$")

def serializar_valor(valor):
    """
    Normaliza valores retornados pelo GeoServer para string PostgreSQL.
    - Arrays Java nao serializados '[Ljava.lang.X;@hash' -> None
    - Listas Python -> string separada por virgula
    - Dicts -> JSON string
    """
    if valor is None:
        return None
    if isinstance(valor, str) and JAVA_ARRAY_RE.match(valor):
        return None
    if isinstance(valor, list):
        partes = [str(v) for v in valor if v is not None]
        return ", ".join(partes) if partes else None
    if isinstance(valor, dict):
        return json.dumps(valor, ensure_ascii=False)
    return str(valor)


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/camadas", response_model=list[Camada], summary="Listar camadas disponiveis no WMS")
async def listar_camadas():
    caps_url = f"{WMS_URL}?SERVICE=WMS&REQUEST=GetCapabilities&VERSION=1.3.0"
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(caps_url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"Erro ao consultar WMS: {e}")

    root = ET.fromstring(resp.content)
    ns = {"wms": "http://www.opengis.net/wms"}
    camadas = []
    for layer in root.findall(".//wms:Layer/wms:Layer", ns):
        name     = layer.findtext("wms:Name",     namespaces=ns)
        title    = layer.findtext("wms:Title",    namespaces=ns)
        abstract = layer.findtext("wms:Abstract", namespaces=ns)
        crs_list = [c.text for c in layer.findall("wms:CRS", ns) if c.text]
        if name:
            camadas.append(Camada(name=name, title=title or "", abstract=abstract, crs=crs_list))

    if not camadas:
        raise HTTPException(status_code=404, detail="Nenhuma camada encontrada no WMS.")
    return camadas


@router.post("/importar", response_model=ImportResult, summary="Importar camada WFS -> PostgreSQL")
async def importar_camada(
    layer: str = Query(..., description="Nome da camada, ex: painel_do_fogo:focos_de_calor"),
    tabela: Optional[str] = Query(None, description="Nome da tabela destino"),
    crs: str = Query("EPSG:4674", description="CRS de saida"),
    max_features: int = Query(10000, description="Limite de feicoes"),
):
    ensure_schema()
    table_name = tabela or layer.split(":")[-1]

    wfs_url = (
        f"{WFS_URL}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature"
        f"&typeName={layer}&outputFormat=application/json"
        f"&srsName={crs}&count={max_features}"
    )

    async with httpx.AsyncClient(timeout=60) as client:
        try:
            resp = await client.get(wfs_url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"Erro ao buscar WFS: {e}")

    geojson  = resp.json()
    features = geojson.get("features", [])
    if not features:
        raise HTTPException(status_code=404, detail=f"Nenhuma feicao retornada para '{layer}'.")

    sample_props = features[0].get("properties", {})
    columns = [c for c in sample_props.keys() if c.lower() != "id"]

    col_defs   = ", ".join(f'"{c}" TEXT' for c in columns)
    full_table = f'{PG_SCHEMA}."{table_name}"'

    create_sql = f"""
        DROP TABLE IF EXISTS {full_table};
        CREATE TABLE {full_table} (
            id SERIAL PRIMARY KEY,
            {col_defs},
            geom GEOMETRY
        );
    """
    insert_sql = f"""
        INSERT INTO {full_table} ({', '.join(f'"{c}"' for c in columns)}, geom)
        VALUES ({', '.join('%s' for _ in columns)}, ST_GeomFromGeoJSON(%s))
    """

    total = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            for feat in features:
                props = feat.get("properties", {})
                geom  = feat.get("geometry")
                if geom is None:
                    continue
                valores = [serializar_valor(props.get(c)) for c in columns]
                valores.append(json.dumps(geom))
                cur.execute(insert_sql, valores)
                total += 1
        conn.commit()

    return ImportResult(layer=layer, table=table_name, schema=PG_SCHEMA, total_inserido=total, status="sucesso")


class ImportTudoResult(BaseModel):
    total_camadas: int
    sucesso: list[ImportResult]
    erros: list[dict]


@router.post("/importar-tudo", response_model=ImportTudoResult, summary="Importar todas as camadas WFS → PostgreSQL")
async def importar_tudo(
    crs: str = Query("EPSG:4674", description="CRS de saída"),
    max_features: int = Query(10000, description="Limite de feições por camada"),
):
    """
    Lista todas as camadas do WMS e importa cada uma para o PostgreSQL.
    Camadas que não suportam WFS (ex: raster) são registradas em 'erros' e ignoradas.
    """
    ensure_schema()

    # ── 1. Listar camadas ──────────────────────────────────────────────────
    caps_url = f"{WMS_URL}?SERVICE=WMS&REQUEST=GetCapabilities&VERSION=1.3.0"
    async with httpx.AsyncClient(timeout=30, verify=False) as client:
        try:
            resp = await client.get(caps_url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"Erro ao consultar WMS: {e}")

    root = ET.fromstring(resp.content)
    ns   = {"wms": "http://www.opengis.net/wms"}
    camadas = []
    for layer in root.findall(".//wms:Layer/wms:Layer", ns):
        name = layer.findtext("wms:Name", namespaces=ns)
        if name:
            camadas.append(name)

    sucesso = []
    erros   = []

    # ── 2. Importar cada camada ────────────────────────────────────────────
    for layer_name in camadas:
        table_name = layer_name.split(":")[-1]
        full_table = f'{PG_SCHEMA}."{table_name}"'

        wfs_url = (
            f"{WFS_URL}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature"
            f"&typeName={layer_name}&outputFormat=application/json"
            f"&srsName={crs}&count={max_features}"
        )

        try:
            async with httpx.AsyncClient(timeout=120, verify=False) as client:
                resp = await client.get(wfs_url)
                resp.raise_for_status()

            features = resp.json().get("features", [])

            if not features:
                erros.append({"layer": layer_name, "erro": "Nenhuma feição retornada (pode ser raster)"})
                continue

            sample_props = features[0].get("properties", {})
            columns = [c for c in sample_props.keys() if c.lower() != "id"]

            col_defs   = ", ".join(f'"{c}" TEXT' for c in columns)
            create_sql = f"""
                DROP TABLE IF EXISTS {full_table};
                CREATE TABLE {full_table} (
                    id SERIAL PRIMARY KEY,
                    {col_defs},
                    geom GEOMETRY
                );
            """
            insert_sql = f"""
                INSERT INTO {full_table} ({', '.join(f'"{c}"' for c in columns)}, geom)
                VALUES ({', '.join('%s' for _ in columns)}, ST_GeomFromGeoJSON(%s))
            """

            total = 0
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_sql)
                    for feat in features:
                        props = feat.get("properties", {})
                        geom  = feat.get("geometry")
                        if geom is None:
                            continue
                        valores = [serializar_valor(props.get(c)) for c in columns]
                        valores.append(json.dumps(geom))
                        cur.execute(insert_sql, valores)
                        total += 1
                conn.commit()

            sucesso.append(ImportResult(
                layer=layer_name,
                table=table_name,
                schema=PG_SCHEMA,
                total_inserido=total,
                status="sucesso",
            ))

        except Exception as e:
            erros.append({"layer": layer_name, "erro": str(e)})

    return ImportTudoResult(
        total_camadas=len(camadas),
        sucesso=sucesso,
        erros=erros,
    )
