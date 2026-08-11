import asyncio
import logging
import re
import secrets

from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

import database
from queue_worker import fila_global

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("main")

app = FastAPI(title="Facta CLT Simulador API", version="2.0.0")


@app.on_event("startup")
async def startup():
    database.init_db()
    await fila_global.iniciar()
    logger.info("API iniciada.")


# ============================================================
# AUTH POR EQUIPE
# ============================================================

async def get_equipe_atual(x_team_key: str = Header(..., alias="X-Team-Key")) -> dict:
    equipe = database.get_equipe(x_team_key)
    if not equipe:
        raise HTTPException(status_code=401, detail="X-Team-Key invalida ou equipe inativa")
    return equipe


# ============================================================
# MODELOS
# ============================================================

class SimularRequest(BaseModel):
    cpf: str = Field(..., description="CPF do cliente, somente numeros")
    token: str = Field(..., description="Token Bearer da Facta (sem o prefixo 'Bearer ')")
    tentativas: int = Field(default=5, ge=1, le=10)

    @field_validator("cpf")
    @classmethod
    def valida_cpf(cls, v: str) -> str:
        v = re.sub(r"\D", "", v)
        if len(v) != 11:
            raise ValueError("cpf deve conter 11 digitos")
        return v


class NovaEquipeRequest(BaseModel):
    nome: str


class ConcorrenciaRequest(BaseModel):
    concorrencia: int = Field(..., ge=1, le=50)


# ============================================================
# ENDPOINTS - SIMULACAO (enfileirada)
# ============================================================

@app.post("/simular_facta_clt")
async def simular_facta_clt(payload: SimularRequest, equipe: dict = Depends(get_equipe_atual)):
    """
    Enfileira a simulacao (nao processa na hora). Retorna job_id para
    consultar o resultado depois em /job/{job_id}.
    Isso evita que muitas chamadas simultaneas (ex: varios fluxos do n8n)
    sobrecarreguem a Facta de uma vez - o processamento real respeita o
    limite de concorrencia configurado no painel.
    """
    job_id = await fila_global.enfileirar(
        equipe=equipe["nome"], cpf=payload.cpf, token=payload.token, tentativas=payload.tentativas
    )
    return {"job_id": job_id, "status": "na_fila", "posicao_fila": fila_global.tamanho_fila()}


@app.get("/job/{job_id}")
async def status_job(job_id: str, equipe: dict = Depends(get_equipe_atual)):
    job = fila_global.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job_id nao encontrado")
    return {
        "job_id": job.job_id,
        "status": job.status,
        "resultado": job.resultado,
    }


@app.post("/simular_facta_clt/aguardar")
async def simular_facta_clt_aguardar(
    payload: SimularRequest, equipe: dict = Depends(get_equipe_atual), timeout: int = 120
):
    """
    Mesmo fluxo, mas aguarda o resultado antes de responder (uso mais simples
    para quem nao quer fazer polling). Sujeito ao timeout informado.
    """
    job_id = await fila_global.enfileirar(
        equipe=equipe["nome"], cpf=payload.cpf, token=payload.token, tentativas=payload.tentativas
    )
    job = fila_global.get_job(job_id)
    try:
        await asyncio.wait_for(job.evento.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=202, detail={"job_id": job_id, "status": "ainda_processando"})

    if job.resultado and job.resultado.get("status") == "ok":
        return job.resultado
    raise HTTPException(status_code=502, detail=job.resultado)


# ============================================================
# ENDPOINTS - GESTAO DE EQUIPES
# ============================================================

@app.post("/admin/equipes")
async def criar_equipe(payload: NovaEquipeRequest):
    api_key = secrets.token_hex(16)
    database.criar_equipe(api_key, payload.nome)
    return {"nome": payload.nome, "api_key": api_key}


@app.get("/admin/equipes")
async def listar_equipes():
    return database.listar_equipes()


# ============================================================
# ENDPOINTS - CONFIG (concorrencia da fila)
# ============================================================

@app.get("/admin/config")
async def get_config():
    return {
        "concorrencia": fila_global.concorrencia_atual(),
        "tamanho_fila_atual": fila_global.tamanho_fila(),
    }


@app.post("/admin/config/concorrencia")
async def set_concorrencia(payload: ConcorrenciaRequest):
    novo_valor = await fila_global.atualizar_concorrencia(payload.concorrencia)
    return {"concorrencia": novo_valor}


# ============================================================
# ENDPOINTS - STATS / DASHBOARD
# ============================================================

@app.get("/admin/stats/hoje")
async def stats_hoje(equipe: str | None = None):
    return database.stats_hoje(equipe)


@app.get("/admin/requisicoes")
async def ultimas_requisicoes(limite: int = 50, equipe: str | None = None):
    return database.ultimas_requisicoes(limite, equipe)


@app.get("/health")
async def health():
    return {"status": "ok", "fila": fila_global.tamanho_fila(), "concorrencia": fila_global.concorrencia_atual()}


# ============================================================
# FRONT (dashboard estatico simples)
# ============================================================

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def dashboard():
    return FileResponse("static/index.html")
