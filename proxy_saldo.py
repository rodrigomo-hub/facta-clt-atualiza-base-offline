"""
Saldo de trafego do proxy residencial (Proxy-Seller): consulta a API do
provedor de hora em hora, guarda fotos no banco, estima consumo/custo e
avisa por webhook quando o saldo estiver baixo ou zerado.

API: GET {API_BASE}/{API_KEY}/resident/package -> traffic_limit,
traffic_usage, traffic_left (bytes). A chave vai na URL, entao NUNCA pode
aparecer em log/erro - ver _sem_chave().
"""
import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import httpx

import database
import queue_worker

# O httpx loga a URL de cada request em INFO - com a API key no caminho isso
# vazaria a chave nos logs do container.
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("proxy_saldo")

API_BASE = os.getenv("PROXYSELLER_API_BASE", "https://proxy-seller.com/personal/api/v2")
INTERVALO_MINUTOS = float(os.getenv("PROXY_SALDO_INTERVALO_MIN", "60"))
ALERTA_PADRAO_MB = 500.0
REPETIR_ALERTA_HORAS = 24
MIB = 1024 ** 2
GIB = 1024 ** 3


# ------------------------------------------------------------------
# Config (banco primeiro, env como fallback para chave e webhook)
# ------------------------------------------------------------------

def get_cfg() -> dict:
    def _f(chave, padrao):
        v = database.get_config(chave, "")
        try:
            return float(v) if v != "" else padrao
        except ValueError:
            return padrao

    return {
        "api_key": database.get_config("proxyseller_api_key", "") or os.getenv("PROXYSELLER_API_KEY", ""),
        "alerta_mb": _f("proxy_alerta_mb", ALERTA_PADRAO_MB),
        "preco_gb": _f("proxy_preco_gb", None),
        "alerta_webhook": database.get_config("proxy_alerta_webhook", "") or os.getenv("ALERTA_WEBHOOK_URL", ""),
    }


def get_cfg_publico() -> dict:
    c = get_cfg()
    return {
        "api_key_configurada": bool(c["api_key"]),
        "alerta_mb": c["alerta_mb"],
        "preco_gb": c["preco_gb"],
        "alerta_webhook": c["alerta_webhook"],
    }


def set_cfg(api_key: Optional[str] = None, alerta_mb: Optional[float] = None,
            preco_gb: Optional[float] = None, alerta_webhook: Optional[str] = None):
    """None = nao mexe. api_key vazia e ignorada (nao apaga). alerta_webhook "" limpa."""
    if api_key is not None and api_key.strip():
        database.set_config("proxyseller_api_key", api_key.strip())
    if alerta_mb is not None:
        database.set_config("proxy_alerta_mb", str(alerta_mb))
    if preco_gb is not None:
        database.set_config("proxy_preco_gb", str(preco_gb))
    if alerta_webhook is not None:
        database.set_config("proxy_alerta_webhook", alerta_webhook.strip())


# ------------------------------------------------------------------
# Consulta a API do provedor
# ------------------------------------------------------------------

def _sem_chave(texto: str, chave: str) -> str:
    return texto.replace(chave, "***") if chave else texto


def _achar(obj, chave: str):
    """Procura `chave` em qualquer nivel do JSON (a docs nao fixa o aninhamento)."""
    if isinstance(obj, dict):
        if chave in obj:
            return obj[chave]
        for v in obj.values():
            r = _achar(v, chave)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _achar(v, chave)
            if r is not None:
                return r
    return None


def _para_int(v) -> Optional[int]:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


async def consultar_api(api_key: str) -> dict:
    """Devolve {traffic_limit, traffic_usage, traffic_left} em bytes. Levanta RuntimeError com mensagem sem a chave."""
    url = f"{API_BASE}/{api_key}/resident/package"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
        corpo = resp.json()
    except Exception as e:
        raise RuntimeError(_sem_chave(f"{type(e).__name__}: {e}", api_key)) from None

    left = _para_int(_achar(corpo, "traffic_left"))
    usage = _para_int(_achar(corpo, "traffic_usage"))
    limite = _para_int(_achar(corpo, "traffic_limit"))
    if left is None and usage is None and limite is None:
        erros = _achar(corpo, "errors") or _achar(corpo, "message") or corpo
        raise RuntimeError(_sem_chave(f"Resposta sem dados de trafego (HTTP {resp.status_code}): {str(erros)[:200]}", api_key))
    if left is None and limite is not None and usage is not None:
        left = max(limite - usage, 0)
    return {"traffic_limit": limite, "traffic_usage": usage, "traffic_left": left}


# ------------------------------------------------------------------
# Estimativas
# ------------------------------------------------------------------

def _parse(iso: str) -> datetime:
    return datetime.fromisoformat(iso)


def resumo() -> dict:
    """Saldo atual + consumo, dias restantes, bytes por CPF e custo (se houver preco)."""
    cfg = get_cfg()
    ultimo = database.ultimo_saldo()
    base = {
        "configurado": bool(cfg["api_key"]),
        "ultima_consulta": ultimo["consultado_em"] if ultimo else None,
        "ultimo_erro": database.get_config("proxy_saldo_ultimo_erro", "") or None,
        "nivel": database.get_config("proxy_saldo_nivel", "") or None,
    }
    if not ultimo:
        return base

    lim, uso, resta = ultimo["traffic_limit"], ultimo["traffic_usage"], ultimo["traffic_left"]
    base.update({
        "traffic_limit_mb": round(lim / MIB, 1) if lim is not None else None,
        "traffic_usage_mb": round(uso / MIB, 1) if uso is not None else None,
        "traffic_left_mb": round(resta / MIB, 1) if resta is not None else None,
        "percentual_restante": round(resta / lim * 100, 1) if lim and resta is not None else None,
    })

    # Janela de estimativa: ate 7 dias de fotos (precisa de >= 2 fotos).
    agora = datetime.utcnow()
    fotos = database.saldos_desde((agora - timedelta(days=7)).isoformat())
    fotos = [f for f in fotos if f["traffic_usage"] is not None]
    if len(fotos) >= 2:
        ref, fim = fotos[0], fotos[-1]
        horas = (_parse(fim["consultado_em"]) - _parse(ref["consultado_em"])).total_seconds() / 3600
        delta = fim["traffic_usage"] - ref["traffic_usage"]
        if horas >= 0.5 and delta >= 0:  # delta < 0 = pacote renovado/zerado no meio da janela
            por_dia = delta / horas * 24
            cpfs = database.contar_requisicoes_proxy_desde(ref["consultado_em"])
            est = {
                "janela_horas": round(horas, 1),
                "consumo_janela_mb": round(delta / MIB, 2),
                "consumo_medio_dia_mb": round(por_dia / MIB, 2),
                "dias_restantes": round(resta / por_dia, 1) if por_dia > 0 and resta is not None else None,
                "cpfs_na_janela": cpfs,
                "kb_por_cpf": round(delta / cpfs / 1024, 1) if cpfs else None,
                "obs": "consumo inclui qualquer outro uso da mesma conta de proxy (ex: testes no n8n)",
            }
            if cfg["preco_gb"] is not None:
                est["custo_janela"] = round(delta / GIB * cfg["preco_gb"], 4)
                est["custo_medio_dia"] = round(por_dia / GIB * cfg["preco_gb"], 4)
                est["custo_por_1000_cpfs"] = round(delta / cpfs * 1000 / GIB * cfg["preco_gb"], 4) if cpfs else None
            base["estimativa"] = est
    if cfg["preco_gb"] is not None and uso is not None:
        base["custo_total_ja_consumido"] = round(uso / GIB * cfg["preco_gb"], 4)
    return base


# ------------------------------------------------------------------
# Atualizacao + alerta
# ------------------------------------------------------------------

def _nivel(resta: Optional[int], alerta_mb: float) -> str:
    if resta is None:
        return "desconhecido"
    if resta <= 0:
        return "zerado"
    if resta / MIB <= alerta_mb:
        return "baixo"
    return "ok"


async def atualizar_saldo() -> dict:
    """Consulta a API, guarda a foto e dispara alerta se precisar. Nunca levanta."""
    cfg = get_cfg()
    if not cfg["api_key"]:
        return {"ok": False, "motivo": "api_key nao configurada"}
    try:
        dados = await consultar_api(cfg["api_key"])
    except RuntimeError as e:
        database.set_config("proxy_saldo_ultimo_erro", str(e))
        logger.warning(f"Falha ao consultar saldo do proxy: {e}")
        return {"ok": False, "motivo": str(e)}

    database.set_config("proxy_saldo_ultimo_erro", "")
    database.salvar_saldo(dados["traffic_limit"], dados["traffic_usage"], dados["traffic_left"])
    nivel = _nivel(dados["traffic_left"], cfg["alerta_mb"])
    database.set_config("proxy_saldo_nivel", nivel)
    try:
        await _checar_alerta(nivel, dados, cfg)
    except Exception:
        logger.exception("Erro ao checar/enviar alerta de saldo do proxy")
    return {"ok": True, **dados, "nivel": nivel}


async def _checar_alerta(nivel: str, dados: dict, cfg: dict):
    if nivel not in ("baixo", "zerado"):
        database.set_config("proxy_alerta_nivel", "ok")  # saldo voltou: rearma o alerta
        return
    ultimo_nivel = database.get_config("proxy_alerta_nivel", "ok")
    enviado_em = database.get_config("proxy_alerta_enviado_em", "")
    if ultimo_nivel == nivel and enviado_em:
        if datetime.utcnow() - _parse(enviado_em) < timedelta(hours=REPETIR_ALERTA_HORAS):
            return  # mesmo nivel, ja avisado nas ultimas 24h
    url = cfg["alerta_webhook"]
    if not url:
        return
    r = resumo()
    dias = (r.get("estimativa") or {}).get("dias_restantes")
    resta_mb = round((dados["traffic_left"] or 0) / MIB, 1)
    msg = (
        "Saldo do proxy ZERADO - as consultas a Facta vao falhar ate recarregar."
        if nivel == "zerado"
        else f"Saldo do proxy baixo: {resta_mb} MB restantes" + (f" (~{dias} dias)" if dias is not None else "") + "."
    )
    payload = {"tipo": "proxy_saldo", "nivel": nivel, "traffic_left_mb": resta_mb,
               "dias_restantes_estimado": dias, "mensagem": msg}
    queue_worker.validar_url_webhook(url)
    if not queue_worker.PERMITIR_REDE_PRIVADA:
        await asyncio.to_thread(queue_worker._resolve_e_checa, url)
    async with httpx.AsyncClient(timeout=10, follow_redirects=False) as client:
        resp = await client.post(url, json=payload)
    if 200 <= resp.status_code < 300:
        database.set_config("proxy_alerta_nivel", nivel)
        database.set_config("proxy_alerta_enviado_em", datetime.utcnow().isoformat())
        logger.warning(f"Alerta de saldo do proxy enviado ({nivel})")
    else:
        logger.warning(f"Alerta de saldo recusado pelo webhook: HTTP {resp.status_code} (tenta de novo na proxima consulta)")


async def monitor():
    """Loop de fundo: consulta o saldo a cada INTERVALO_MINUTOS (so quando ha api_key)."""
    await asyncio.sleep(30)
    while True:
        try:
            if get_cfg()["api_key"]:
                await atualizar_saldo()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Erro no monitor de saldo do proxy")
        await asyncio.sleep(max(INTERVALO_MINUTOS, 1) * 60)
