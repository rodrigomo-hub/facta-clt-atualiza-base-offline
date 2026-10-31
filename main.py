import asyncio
import logging
import os
import re
import secrets
from typing import Optional

from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

import database
import proxy_saldo
from queue_worker import fila_global, validar_url_webhook, MAX_CONCORRENCIA

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("main")

app = FastAPI(title="Facta CLT Simulador API", version="2.0.0")


@app.on_event("startup")
async def startup():
    database.init_db()
    await fila_global.iniciar()
    app.state.monitor_saldo = asyncio.create_task(proxy_saldo.monitor())
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
# AUTH DO PAINEL ADMIN (Basic Auth)
# ------------------------------------------------------------
# Protege todas as rotas /admin/* - antes estava completamente
# aberto na rede, qualquer um com a URL conseguia criar equipes,
# ver historico de requisicoes e mudar config (incluindo credenciais
# de proxy). Usuario/senha vem de env var, comparado com
# secrets.compare_digest para evitar timing attack.
# ============================================================

_security = HTTPBasic()
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "")


async def exigir_admin(credentials: HTTPBasicCredentials = Depends(_security)):
    if not ADMIN_PASS:
        # Falha segura: se ninguem configurou ADMIN_PASS, bloqueia tudo em
        # vez de deixar aberto sem querer.
        raise HTTPException(
            status_code=503,
            detail="ADMIN_PASS nao configurado no ambiente - painel admin bloqueado por seguranca.",
        )
    usuario_ok = secrets.compare_digest(credentials.username, ADMIN_USER)
    senha_ok = secrets.compare_digest(credentials.password, ADMIN_PASS)
    if not (usuario_ok and senha_ok):
        raise HTTPException(
            status_code=401,
            detail="Credenciais invalidas",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


# ============================================================
# MODELOS
# ============================================================

class SimularRequest(BaseModel):
    cpf: str = Field(..., description="CPF do cliente, somente numeros")
    tentativas: int = Field(default=5, ge=1, le=10)
    # Retorno por webhook: o servico responde na hora com o job_id e, quando
    # o CPF terminar de processar, faz POST do resultado nessa URL (sem o
    # chamador ficar esperando). `ref` e devolvido igual no retorno, para
    # ligar a resposta ao lead de quem chamou.
    webhook_url: Optional[str] = Field(default=None, description="URL que recebe o resultado por POST")
    ref: Optional[str] = Field(default=None, max_length=200, description="Texto livre devolvido no webhook")

    @field_validator("webhook_url")
    @classmethod
    def valida_webhook(cls, v: Optional[str]) -> Optional[str]:
        if v is None or not v.strip():
            return None
        return validar_url_webhook(v)

    @field_validator("cpf")
    @classmethod
    def valida_cpf(cls, v: str) -> str:
        v = re.sub(r"\D", "", v)
        if len(v) != 11:
            raise ValueError("cpf deve conter 11 digitos")
        return v


class ResultadoFluxoRequest(BaseModel):
    """Resultado final que o fluxo (n8n) reporta de volta para um job."""
    job_id: str = Field(..., min_length=4, max_length=64)
    resultado: str = Field(..., min_length=2, max_length=40)  # PRE_APROVADO | NAO_APROVADO | FALHA_CONSULTA | ...
    motivo: Optional[str] = Field(None, max_length=200)
    banco: Optional[str] = Field(None, max_length=80)
    margem: Optional[float] = None
    valor_liquido_max: Optional[float] = None
    detalhes: Optional[dict] = None

    @field_validator("resultado")
    @classmethod
    def _normaliza(cls, v: str) -> str:
        v = re.sub(r"[^A-Z0-9_]", "_", v.strip().upper())
        return v or "DESCONHECIDO"


class NovaEquipeRequest(BaseModel):
    nome: str


class PrioridadeEquipeRequest(BaseModel):
    api_key: str
    prioridade: int = Field(..., ge=0, le=10)


class ConcorrenciaRequest(BaseModel):
    # Teto original era 50 - com proxy residencial ligado, cada chamada
    # individual fica bem mais lenta (~1.9s vs ~0.2s direto), entao o
    # gargalo passa a ser latencia de rede por worker, nao mais rate
    # limit. Subir o teto de workers compensa essa latencia mantendo
    # throughput agregado alto.
    concorrencia: int = Field(..., ge=1, le=MAX_CONCORRENCIA)


class SaldoProxyConfigRequest(BaseModel):
    # Campos None = nao mexe. api_key vazia e ignorada (nao apaga a salva).
    api_key: Optional[str] = None
    alerta_mb: Optional[float] = Field(default=None, ge=0)
    preco_gb: Optional[float] = Field(default=None, ge=0)
    alerta_webhook_url: Optional[str] = None  # "" limpa

    @field_validator("alerta_webhook_url")
    @classmethod
    def valida_alerta_webhook(cls, v: Optional[str]) -> Optional[str]:
        if v is None or not v.strip():
            return v if v is None else ""
        return validar_url_webhook(v)


class ProxyConfigRequest(BaseModel):
    usar_proxy: Optional[bool] = None
    proxy_host: Optional[str] = None
    proxy_port: Optional[str] = None
    proxy_user: Optional[str] = None
    proxy_pass: Optional[str] = None


# ============================================================
# ENDPOINTS - SIMULACAO (enfileirada)
# ============================================================

def _recusar_se_pausado():
    """Em pausa o servico NAO aceita CPFs novos (o n8n recebe 503 e segue para o proximo)."""
    if fila_global.pausado():
        raise HTTPException(status_code=503, detail="Servico pausado: nenhum CPF novo e aceito no momento",
                            headers={"Retry-After": "60"})


@app.post("/simular_facta_clt")
async def simular_facta_clt(payload: SimularRequest, equipe: dict = Depends(get_equipe_atual)):
    """
    Enfileira a simulacao (nao processa na hora). Retorna job_id para
    consultar o resultado depois em /job/{job_id}.
    Isso evita que muitas chamadas simultaneas (ex: varios fluxos do n8n)
    sobrecarreguem a Facta de uma vez - o processamento real respeita o
    limite de concorrencia configurado no painel.
    """
    _recusar_se_pausado()
    job_id, ja_existia = await fila_global.enfileirar(
        equipe=equipe["nome"], cpf=payload.cpf, tentativas=payload.tentativas,
        prioridade=equipe.get("prioridade", 0),
        webhook_url=payload.webhook_url, ref=payload.ref,
    )
    return {
        "job_id": job_id,
        "status": "na_fila" if not ja_existia else "ja_estava_na_fila",
        "posicao_fila": fila_global.tamanho_fila(),
        "duplicata_evitada": ja_existia,
        "webhook_registrado": bool(payload.webhook_url),
    }


@app.get("/job/{job_id}")
async def status_job(job_id: str, equipe: dict = Depends(get_equipe_atual)):
    info = fila_global.consultar_job(job_id)
    if not info:
        raise HTTPException(status_code=404, detail="job_id nao encontrado")
    return info


@app.post("/simular_facta_clt/aguardar")
async def simular_facta_clt_aguardar(
    payload: SimularRequest, equipe: dict = Depends(get_equipe_atual), timeout: int = 120
):
    """
    Mesmo fluxo, mas aguarda o resultado antes de responder (uso mais simples
    para quem nao quer fazer polling). Sujeito ao timeout informado.
    """
    _recusar_se_pausado()
    job_id, _ja_existia = await fila_global.enfileirar(
        equipe=equipe["nome"], cpf=payload.cpf, tentativas=payload.tentativas,
        prioridade=equipe.get("prioridade", 0),
        webhook_url=payload.webhook_url, ref=payload.ref,
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
async def criar_equipe(payload: NovaEquipeRequest, _admin: bool = Depends(exigir_admin)):
    api_key = secrets.token_hex(16)
    database.criar_equipe(api_key, payload.nome)
    return {"nome": payload.nome, "api_key": api_key}


@app.get("/admin/equipes")
async def listar_equipes(_admin: bool = Depends(exigir_admin)):
    return database.listar_equipes()


@app.post("/admin/equipes/prioridade")
async def set_prioridade_equipe(payload: PrioridadeEquipeRequest, _admin: bool = Depends(exigir_admin)):
    equipe = database.get_equipe(payload.api_key)
    if not equipe:
        raise HTTPException(status_code=404, detail="Equipe nao encontrada")
    database.atualizar_prioridade_equipe(payload.api_key, payload.prioridade)
    return {"nome": equipe["nome"], "prioridade": payload.prioridade}


# ============================================================
# ENDPOINTS - CONFIG (concorrencia da fila)
# ============================================================

@app.get("/admin/config")
async def get_config(_admin: bool = Depends(exigir_admin)):
    return {
        "concorrencia": fila_global.concorrencia_atual(),
        "tamanho_fila_atual": fila_global.tamanho_fila(),
        "pausado": fila_global.pausado(),
        "max_tentativas": fila_global.max_tentativas(),
    }


class TentativasRequest(BaseModel):
    max_tentativas: int = Field(..., ge=1, le=10)


@app.post("/admin/config/tentativas")
async def set_max_tentativas(payload: TentativasRequest, _admin: bool = Depends(exigir_admin)):
    """Teto global de tentativas por CPF; vence o `tentativas` enviado pelo chamador (vale tambem para jobs ja na fila)."""
    database.set_config("max_tentativas", str(payload.max_tentativas))
    return {"max_tentativas": fila_global.max_tentativas()}


class PausaRequest(BaseModel):
    pausado: bool


class LimparRequest(BaseModel):
    confirmar: str


@app.post("/admin/limpar-tudo")
async def limpar_tudo(payload: LimparRequest, _admin: bool = Depends(exigir_admin)):
    """Apaga TODOS os CPFs/requisicoes, webhooks, resultados e a fila. Exige confirmar="APAGAR"."""
    if payload.confirmar != "APAGAR":
        raise HTTPException(status_code=400, detail='Envie {"confirmar": "APAGAR"}')
    return fila_global.limpar_tudo()


@app.post("/admin/config/pausa")
async def set_pausa(payload: PausaRequest, _admin: bool = Depends(exigir_admin)):
    """Pausa/retoma. Em pausa nenhum job novo comeca E /simular_facta_clt recusa CPFs novos com 503."""
    fila_global.definir_pausa(payload.pausado)
    return {"pausado": fila_global.pausado(), "tamanho_fila_atual": fila_global.tamanho_fila()}


@app.post("/admin/config/concorrencia")
async def set_concorrencia(payload: ConcorrenciaRequest, _admin: bool = Depends(exigir_admin)):
    novo_valor = await fila_global.atualizar_concorrencia(payload.concorrencia)
    return {"concorrencia": novo_valor}


# ============================================================
# ENDPOINTS - CONFIG (proxy residencial)
# ------------------------------------------------------------
# Configuravel via painel/API sem precisar de redeploy. GET nunca
# devolve a senha em texto - so confirma se esta configurada ou nao.
# ============================================================

@app.get("/admin/config/proxy")
async def get_config_proxy(_admin: bool = Depends(exigir_admin)):
    return database.get_proxy_config_publico()


@app.post("/admin/config/proxy")
async def set_config_proxy(payload: ProxyConfigRequest, _admin: bool = Depends(exigir_admin)):
    database.set_proxy_config(
        usar_proxy=payload.usar_proxy,
        proxy_host=payload.proxy_host,
        proxy_port=payload.proxy_port,
        proxy_user=payload.proxy_user,
        proxy_pass=payload.proxy_pass,
    )
    return database.get_proxy_config_publico()


# ============================================================
# ENDPOINTS - STATS / DASHBOARD
# ============================================================

@app.get("/admin/proxy/saldo")
async def saldo_proxy(atualizar: bool = False, _admin: bool = Depends(exigir_admin)):
    """
    Saldo de trafego do proxy (API do Proxy-Seller) + estimativas de consumo,
    dias restantes e custo. `?atualizar=true` consulta a API agora; sem isso
    devolve a ultima foto (o monitor atualiza de hora em hora).
    """
    consulta = await proxy_saldo.atualizar_saldo() if atualizar else None
    out = proxy_saldo.resumo()
    out["config"] = proxy_saldo.get_cfg_publico()
    if consulta is not None:
        out["consulta_agora"] = {k: v for k, v in consulta.items() if k in ("ok", "motivo")}
    return out


@app.post("/admin/proxy/saldo/config")
async def saldo_proxy_config(payload: SaldoProxyConfigRequest, _admin: bool = Depends(exigir_admin)):
    proxy_saldo.set_cfg(
        api_key=payload.api_key, alerta_mb=payload.alerta_mb,
        preco_gb=payload.preco_gb, alerta_webhook=payload.alerta_webhook_url,
    )
    return proxy_saldo.get_cfg_publico()


@app.get("/admin/stats/hoje")
async def stats_hoje(equipe: str | None = None, _admin: bool = Depends(exigir_admin)):
    return database.stats_hoje(equipe)


@app.post("/resultado")
async def registrar_resultado_fluxo(
    payload: ResultadoFluxoRequest,
    x_team_key: Optional[str] = Header(None, alias="X-Team-Key"),
):
    """
    Chamado pelo fluxo do n8n ao terminar, com o desfecho (pre-aprovado, nao
    aprovado + motivo, falha...). A autorizacao e o proprio job_id (aleatorio,
    so conhecido por quem recebeu o webhook). Se X-Team-Key vier, tem que ser
    da equipe dona do job.
    """
    req = database.get_requisicao_por_job(payload.job_id)
    if not req:
        raise HTTPException(status_code=404, detail="job_id nao encontrado")
    if x_team_key:
        equipe = database.get_equipe(x_team_key)
        if not equipe or equipe["nome"] != req["equipe"]:
            raise HTTPException(status_code=403, detail="X-Team-Key nao pertence a equipe do job")
    database.registrar_resultado(
        payload.job_id, payload.resultado, payload.motivo, payload.banco,
        payload.margem, payload.valor_liquido_max, payload.detalhes,
    )
    return {"ok": True, "job_id": payload.job_id, "resultado": payload.resultado}


@app.get("/admin/funil")
async def funil_hoje(equipe: str | None = None, _admin: bool = Depends(exigir_admin)):
    return database.stats_funil_hoje(equipe)


@app.get("/admin/capacidade")
async def capacidade(_admin: bool = Depends(exigir_admin)):
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


@app.get("/admin/webhooks")
async def listar_webhooks(status: str | None = None, limite: int = 50, _admin: bool = Depends(exigir_admin)):
    """Outbox de webhooks de retorno. status: aguardando | pendente | entregue | falhou."""
    return {"contagem": database.contar_webhooks(), "webhooks": database.listar_webhooks(status, limite)}


@app.post("/admin/webhooks/reenviar")
async def reenviar_webhooks(_admin: bool = Depends(exigir_admin)):
    """Recoloca na fila de envio os webhooks que esgotaram as tentativas ('falhou')."""
    return {"reenfileirados": database.reenviar_webhooks_falhos()}


@app.get("/admin/requisicoes")
async def ultimas_requisicoes(limite: int = 50, equipe: str | None = None, _admin: bool = Depends(exigir_admin)):
    return database.ultimas_requisicoes(limite, equipe)


@app.get("/admin/diagnostico-facta")
async def diagnostico_facta(via_proxy: bool = False, _admin: bool = Depends(exigir_admin)):
    """
    Testa uma unica chamada direta ao endpoint da Facta (sem fila, sem retry)
    e devolve status code + headers relevantes na hora. Serve para confirmar
    rapidamente se o IP dessa VPS especifica (ou do proxy, se via_proxy=true)
    esta sendo bloqueado pelo Cloudflare da Facta, sem esperar timeout do
    fluxo completo.
    """
    from curl_cffi import requests as _cffi
    import time as _time
    import facta_clt as _fc

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

    proxy_url = None
    if via_proxy:
        import facta_clt

        proxy_url = facta_clt._montar_proxy_url()
        if not proxy_url:
            return {
                "erro": True,
                "mensagem": "via_proxy=true mas o proxy nao esta configurado/ligado "
                "(confira /admin/config/proxy)",
            }

    inicio = _time.monotonic()
    try:
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        async with _cffi.AsyncSession(impersonate=_fc.IMPERSONATE, proxies=proxies) as client:
            resp = await client.post(
                "https://api-autocontratacao.factafinanceira.com.br/clt/analisa-cliente",
                headers=headers,
                data=body,
                timeout=15,
            )
        duracao = round(_time.monotonic() - inicio, 2)
        eh_challenge_cloudflare = "Just a moment" in resp.text[:500]
        return {
            "via_proxy": via_proxy,
            "status_code": resp.status_code,
            "duracao_segundos": duracao,
            "cf_ray": resp.headers.get("cf-ray"),
            "server": resp.headers.get("server"),
            "eh_challenge_cloudflare": eh_challenge_cloudflare,
            "primeiros_300_chars": resp.text[:300],
        }
    except Exception as e:
        duracao = round(_time.monotonic() - inicio, 2)
        return {"erro": True, "via_proxy": via_proxy, "duracao_segundos": duracao, "mensagem": str(e)}


@app.get("/health")
async def health():
    return {"status": "ok", "fila": fila_global.tamanho_fila(), "concorrencia": fila_global.concorrencia_atual()}


# ============================================================
# FRONT (dashboard estatico simples)
# ============================================================

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def dashboard(_admin: bool = Depends(exigir_admin)):
    return FileResponse("static/index.html")
