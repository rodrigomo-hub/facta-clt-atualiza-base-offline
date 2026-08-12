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


class PrioridadeEquipeRequest(BaseModel):
    api_key: str
    prioridade: int = Field(..., ge=0, le=10)


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
    job_id, ja_existia = await fila_global.enfileirar(
        equipe=equipe["nome"], cpf=payload.cpf, tentativas=payload.tentativas,
        prioridade=equipe.get("prioridade", 0),
    )
    return {
        "job_id": job_id,
        "status": "na_fila" if not ja_existia else "ja_estava_na_fila",
        "posicao_fila": fila_global.tamanho_fila(),
        "duplicata_evitada": ja_existia,
    }


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
    job_id, _ja_existia = await fila_global.enfileirar(
        equipe=equipe["nome"], cpf=payload.cpf, tentativas=payload.tentativas,
        prioridade=equipe.get("prioridade", 0),
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


@app.post("/admin/equipes/prioridade")
async def set_prioridade_equipe(payload: PrioridadeEquipeRequest):
    equipe = database.get_equipe(payload.api_key)
    if not equipe:
        raise HTTPException(status_code=404, detail="Equipe nao encontrada")
    database.atualizar_prioridade_equipe(payload.api_key, payload.prioridade)
    return {"nome": equipe["nome"], "prioridade": payload.prioridade}


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


@app.get("/admin/capacidade")
async def capacidade():
    """
    Calcula throughput teorico (baseado no rate limiter atual) vs uso real
    de hoje, e projeta quanto tempo levaria para processar um volume diario
    dado. Ajuda a planejar se o intervalo/concorrencia atual aguenta o
    volume de leads do dia.
    """
    import facta_clt

    intervalo = facta_clt.INTERVALO_MINIMO_SEGUNDOS
    chamadas_por_hora = round(3600 / intervalo, 1) if intervalo > 0 else None
    chamadas_por_dia_24h = round(chamadas_por_hora * 24, 0) if chamadas_por_hora else None

    capacidade_hoje = database.stats_capacidade_hoje()
    media_chamadas = capacidade_hoje["media_chamadas_por_job"] or 2.0  # assume 2 (auth+sim) se sem dado ainda

    cpfs_por_dia_estimado = (
        round(chamadas_por_dia_24h / media_chamadas) if chamadas_por_dia_24h and media_chamadas else None
    )

    return {
        "intervalo_minimo_segundos": intervalo,
        "chamadas_por_hora_teorico": chamadas_por_hora,
        "chamadas_por_dia_teorico_24h": chamadas_por_dia_24h,
        "media_chamadas_por_cpf_hoje": media_chamadas,
        "cpfs_por_dia_estimado_rodando_24h": cpfs_por_dia_estimado,
        "total_chamadas_http_hoje": capacidade_hoje["total_chamadas_http_hoje"],
        "total_jobs_processados_hoje": capacidade_hoje["total_jobs_processados_hoje"],
        "tamanho_fila_atual": fila_global.tamanho_fila(),
    }


@app.get("/admin/requisicoes")
async def ultimas_requisicoes(limite: int = 50, equipe: str | None = None):
    return database.ultimas_requisicoes(limite, equipe)


@app.get("/admin/diagnostico-facta")
async def diagnostico_facta():
    """
    Testa uma unica chamada direta ao endpoint da Facta (sem fila, sem retry)
    e devolve status code + headers relevantes na hora. Serve para confirmar
    rapidamente se o IP dessa VPS especifica esta sendo bloqueado pelo
    Cloudflare da Facta, sem esperar timeout do fluxo completo.
    """
    import httpx as _httpx
    import time as _time

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "application/json",
        "Origin": "https://autocontratacao.factafinanceira.com.br",
        "Referer": "https://autocontratacao.factafinanceira.com.br/",
    }
    body = {
        "nome": "Teste Diagnostico",
        "cpf": "00000000000",
        "celular": "5199999999",
        "email": "teste.diagnostico@gmail.com",
        "termos": "S",
        "ip": "189.45.67.88",
    }

    inicio = _time.monotonic()
    try:
        async with _httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api-autocontratacao.factafinanceira.com.br/clt/analisa-cliente",
                headers=headers,
                data=body,
            )
        duracao = round(_time.monotonic() - inicio, 2)
        eh_challenge_cloudflare = "Just a moment" in resp.text[:500]
        return {
            "status_code": resp.status_code,
            "duracao_segundos": duracao,
            "cf_ray": resp.headers.get("cf-ray"),
            "server": resp.headers.get("server"),
            "eh_challenge_cloudflare": eh_challenge_cloudflare,
            "primeiros_300_chars": resp.text[:300],
        }
    except Exception as e:
        duracao = round(_time.monotonic() - inicio, 2)
        return {"erro": True, "duracao_segundos": duracao, "mensagem": str(e)}


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
