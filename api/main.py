from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import wms, dados
from config import ensure_schema

app = FastAPI(
    title="SIPAM Painel do Fogo API",
    description="API para consulta, importação e exposição de camadas geoespaciais do SIPAM.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    """Garante schema e extensão PostGIS no startup."""
    try:
        ensure_schema()
    except Exception as e:
        print(f"[AVISO] Não foi possível verificar o schema no banco: {e}")


app.include_router(wms.router)
app.include_router(dados.router)


@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "docs": "/docs"}
