from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from config import PG_SCHEMA, get_conn

router = APIRouter(prefix="/dados", tags=["Dados"])


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/tabelas", summary="Listar tabelas salvas no schema do PostGIS")
def listar_tabelas():
    """
    Retorna todas as tabelas geográficas salvas no schema configurado.
    """
    sql = """
        SELECT
            table_name,
            (SELECT COUNT(*) FROM information_schema.columns c
             WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS total_colunas
        FROM information_schema.tables t
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (PG_SCHEMA,))
            rows = cur.fetchall()

    return {"schema": PG_SCHEMA, "tabelas": [dict(r) for r in rows]}


@router.get("/{tabela}", summary="Consultar registros de uma tabela")
def consultar_tabela(
    tabela: str,
    limit: int = Query(100, le=5000, description="Limite de registros"),
    offset: int = Query(0, description="Offset para paginação"),
):
    """
    Retorna registros de uma tabela salva, sem geometria (apenas atributos).
    """
    full_table = f'{PG_SCHEMA}."{tabela}"'

    # Verifica se a tabela existe
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s)",
                (PG_SCHEMA, tabela),
            )
            existe = cur.fetchone()["exists"]

    if not existe:
        raise HTTPException(status_code=404, detail=f"Tabela '{tabela}' não encontrada no schema '{PG_SCHEMA}'.")

    sql = f"""
        SELECT * FROM {full_table}
        WHERE geom IS NOT NULL
        LIMIT %s OFFSET %s
    """
    count_sql = f"SELECT COUNT(*) AS total FROM {full_table}"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql)
            total = cur.fetchone()["total"]

            cur.execute(sql, (limit, offset))
            rows = cur.fetchall()

    # Remove coluna geom da resposta tabular
    registros = []
    for row in rows:
        r = dict(row)
        r.pop("geom", None)
        registros.append(r)

    return {
        "tabela": tabela,
        "schema": PG_SCHEMA,
        "total": total,
        "limit": limit,
        "offset": offset,
        "registros": registros,
    }


_geojson_cache: dict[str, dict] = {}


@router.get("/{tabela}/geojson", summary="Expor tabela como GeoJSON")
def geojson_tabela(
    tabela: str,
    limit: int = Query(1000, le=10000, description="Limite de feições"),
    offset: int = Query(0, description="Offset para paginação"),
    bbox: Optional[str] = Query(
        None,
        description="Filtro por bounding box: minx,miny,maxx,maxy (EPSG:4674)"
    ),
    simplify: float = Query(0, description="Tolerância de simplificação em graus (ex: 0.001)"),
):
    """
    Retorna os dados da tabela como FeatureCollection GeoJSON válido.
    Suporta filtro por bbox e simplificação de geometrias.
    """
    # Cache key (apenas para requests sem bbox)
    cache_key = f"{tabela}:{limit}:{offset}:{simplify}" if not bbox else ""
    if cache_key and cache_key in _geojson_cache:
        return JSONResponse(content=_geojson_cache[cache_key], media_type="application/geo+json")

    full_table = f'{PG_SCHEMA}."{tabela}"'

    bbox_filter = ""
    bbox_params: list = []

    if bbox:
        try:
            minx, miny, maxx, maxy = [float(v) for v in bbox.split(",")]
        except ValueError:
            raise HTTPException(status_code=400, detail="bbox inválido. Use: minx,miny,maxx,maxy")

        bbox_filter = """
            AND geom && ST_MakeEnvelope(%s, %s, %s, %s, 4674)
        """
        bbox_params = [minx, miny, maxx, maxy]

    # Simplificação opcional de geometrias (reduz vértices)
    geom_expr = "ST_Transform(geom, 4326)"
    if simplify > 0:
        geom_expr = f"ST_Simplify(ST_Transform(geom, 4326), {simplify}, true)"

    # Query otimizada: sem subquery N+1, usa to_jsonb direto
    sql = f"""
        SELECT
            id,
            ST_AsGeoJSON({geom_expr})::json AS geometry,
            to_jsonb(t) - 'geom' AS properties
        FROM {full_table} t
        WHERE geom IS NOT NULL
        {bbox_filter}
        LIMIT %s OFFSET %s
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, bbox_params + [limit, offset])
            rows = cur.fetchall()

    features = []
    for row in rows:
        features.append({
            "type": "Feature",
            "geometry": row["geometry"],
            "properties": dict(row["properties"]),
        })

    geojson = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
        "total": len(features),
        "features": features,
    }

    # Cachear resultado
    if cache_key:
        _geojson_cache[cache_key] = geojson

    return JSONResponse(content=geojson, media_type="application/geo+json")
